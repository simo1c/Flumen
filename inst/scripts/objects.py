import struct
import re
from collections import namedtuple
import pyarrow as pa
import pyarrow.ipc
import pandas as pd
import numpy as np

# Return shape of serialize_result: keep self-documenting at call sites
# (result.data, result.data_type, result.elem_names) instead of a bare tuple.
SerializedResult = namedtuple('SerializedResult', ['data', 'data_type', 'elem_names'])

# Global constant to avoid redundant type-checking and centralize supported types
SUPPORTED_TYPES = (pd.DataFrame, pa.Table, np.ndarray)

# Fixed metadata fields the supervisor prepends to every code+parameters dispatch
# message (one row each in the envelope, in this order). Mirror of
# R/supervisor_lib.R::FIXED_DISPATCH_FIELD_NAMES -- keep the two in sync.
FIXED_DISPATCH_FIELD_NAMES = ('shell_interpreter', 'user_code', 'output_files',
                              'is_output_par', 'dyn_wildcards',
                              'output_auto_mkdir', 'output_par_names',
                              'output_object_par_names',
                              # the declared FORM of every parameter
                              # ("bare", "list", "aggregate"), in slot order
                              'params_form')
N_FIXED_DISPATCH_FIELDS = len(FIXED_DISPATCH_FIELD_NAMES)


def object_output_labels(out_object_par_names, n_results, elem_names=None):
    """Which declared output each OBJECT result belongs to.

    Mirror of R/wrapper_lib.R::wrapper_object_output_labels -- keep the two in
    sync. The discriminator is HOW MANY object outputs the task DECLARED, which
    is what the output_object_par_names dispatch field states, not how many
    results came back: one dynamic object output returning three values gives
    three results that all belong to that single output.
    """
    declared = list(out_object_par_names)
    if len(declared) == 1:
        # ONE declared object output: every returned value belongs to it,
        # whatever the return's own keys say. Under a dynamic output those keys
        # are the WILDCARD VALUES, never the output's name.
        return [str(declared[0])] * n_results
    if len(declared) > 1 and elem_names is not None and len(elem_names) == n_results:
        # SEVERAL declared object outputs: by contract the keys of the returned
        # dict ARE the output names, so the producer has already stated it.
        return [str(n) for n in elem_names]
    # Several declared object outputs and an unnamed return: the label must stay
    # empty, because that return is ambiguous and is refused downstream by name.
    return [''] * n_results


def extract_multiple_wildcard_values(pattern_vector, values_vector):
    """Port of R/wildcard_engine.R::extract_multiple_wildcard_values.

    For each `{{wc}}` pattern in `pattern_vector`, build a regex (escaping
    other regex metacharacters but NOT braces) and match it against the
    strings in `values_vector`. Returns the matched strings and the per-
    wildcard captured values across all patterns.

    Used by the Python wrapper to enumerate produced files post-execution
    (same role as in wrapper.R), so Python file outputs work via side-effect
    -- on par with R.
    """
    if isinstance(pattern_vector, str):
        pattern_vector = [pattern_vector]
    if isinstance(values_vector, str):
        values_vector = [values_vector]

    all_w_res = {}
    all_match = []

    # Same escape set as R: . | ( ) [ ] $ ^ * + ? (braces NOT escaped, so the
    # {{wc}} literal can still be found in reg_mod for replacement).
    META_RE = re.compile(r'([.\\|()\[\]$^*+?])')

    for model in pattern_vector:
        # Find {{wc}} tokens in declaration order, deduplicated.
        seen = set()
        w_names_br = []
        for tok in re.findall(r'\{\{.*?\}\}', model):
            if tok not in seen:
                seen.add(tok)
                w_names_br.append(tok)
        w_names = [re.sub(r'[{}]', '', x) for x in w_names_br]
        if not w_names:
            continue

        reg_mod = META_RE.sub(r'\\\1', model)
        # Replace each {{wc}} with a capture group. Last one greedy (.*),
        # others lazy (.*?) -- mirrors R's behaviour for multi-wildcard
        # disambiguation.
        # In step with R/wildcard_engine.R, three rules pin the match:
        #  ABSORB: the capture may cross '/' only when the pattern is RECURSIVE
        #          by is_recursive_wildcard(), the same predicate that chooses
        #          recursive listing for it, so the matcher accepts exactly what
        #          the lister enumerates. Dots stay allowed (idx_base.rev.1.bt2).
        #  START:  anchored at a path-component boundary, (?:^|/), so a relative
        #          pattern matches an absolute path but never mid-name.
        #  END:    anchored at the absolute end, \Z, not $, which also matches
        #          before a trailing newline.
        # This is a port of R/wildcard_engine.R::extract_multiple_wildcard_values
        # and must carry the same three rules: a change there is a change here.
        rec = is_recursive_wildcard(model)
        cap_lazy = '(.*?)' if rec else '([^/]*?)'
        cap_greedy = '(.*)' if rec else '([^/]*)'
        for i, esc in enumerate(w_names_br):
            replacement = cap_lazy if i < len(w_names_br) - 1 else cap_greedy
            reg_mod = reg_mod.replace(esc, replacement, 1)

        compiled = re.compile('(?:^|/)(?:' + reg_mod + r')\Z')
        cur_matches = [v for v in values_vector if compiled.search(v)]
        if not cur_matches:
            continue

        all_match.extend(cur_matches)
        for v in cur_matches:
            mo = compiled.search(v)
            for i, name in enumerate(w_names):
                cap = mo.group(i + 1)
                if name not in all_w_res:
                    all_w_res[name] = []
                if cap not in all_w_res[name]:
                    all_w_res[name].append(cap)

    # Dedup matches preserving order
    seen = set()
    matches_dedup = []
    for m in all_match:
        if m not in seen:
            seen.add(m)
            matches_dedup.append(m)

    return {'wildcards': all_w_res, 'matches': matches_dedup}


def is_recursive_wildcard(pattern):
    """Port of R/wildcard_engine.R::is_recursive_wildcard. A bare dynamic
    wildcard output -- the wildcard with NO extension after it, e.g.
    `dir/`{{any}} -- signals whole-folder intent: glob the directory
    recursively and transfer the whole sub-tree. With an extension
    (`dir/`{{any}}.ext) only matching files travel, kept single-level.
    Returns True iff `pattern` has a dynamic wildcard whose basename ends
    with the closing `}}` (nothing after it)."""
    if not isinstance(pattern, str) or not pattern:
        return False
    base = pattern.replace('`', '').split('/')[-1]
    if not re.search(r'\{\{[^{}]+\}\}', base):
        return False
    return re.sub(r'.*\}\}', '', base) == ''


def decode_message(raw: bytes) -> list:
    """Decode an Arrow IPC binary message.

    Wire format: [4B header_len LE][header Arrow IPC][payload_1]...[payload_N]
    There is no topic prefix: header_len starts at byte 0.
    """
    # Use memoryview to avoid unnecessary byte copies during slicing
    raw_mv = memoryview(raw)

    header_len = struct.unpack_from('<i', raw_mv, 0)[0]
    h_start    = 4

    # Use pa.py_buffer to guarantee zero-copy compatibility without C++ errors
    header_buffer = pa.py_buffer(raw_mv[h_start:h_start + header_len])
    envelope = pa.ipc.open_stream(header_buffer).read_all()

    pos = h_start + header_len
    fields = []

    for i in range(len(envelope)):
        psize = int(envelope['payload_size'][i].as_py())

        # Extract the actual bytes only for the specific payload
        payload = raw_mv[pos:pos + psize].tobytes()
        pos += psize
        
        # elem_name is always populated by new_message (defaults to "") so it
        # is a hard schema invariant; absence would indicate a corrupted
        # payload and should fail loudly here.
        fields.append({
            'sender':     envelope['sender'][i].as_py(),
            'msg_id':     envelope['msg_id'][i].as_py(),
            'msg_type':   envelope['msg_type'][i].as_py(),
            'data_type':  envelope['data_type'][i].as_py(),
            'data_names': envelope['data_names'][i].as_py(),
            'is_arrow':   envelope['is_arrow'][i].as_py(),
            'elem_name':  envelope['elem_name'][i].as_py(),
            'chunk':      int(envelope['chunk'][i].as_py()),
            'n_chunks':   int(envelope['n_chunks'][i].as_py()),
            'tot_size':   int(envelope['tot_size'][i].as_py()),
            # per-element declared size: the length the reassembled payload must
            # have. Carried since chunking existed, never read until merge_chunks
            # started checking it.
            'element_tot_size': int(envelope['element_tot_size'][i].as_py()),
            'data':       payload,
        })
    return fields


def deserialize_field(field: dict):
    """Arrow IPC bytes -> Python scalar (if 1 element) or list."""
    tbl = pa.ipc.open_stream(field['data']).read_all()
    col = tbl.column(0)
    return col[0].as_py() if len(col) == 1 else col.to_pylist()


def deserialize_vector_field(field: dict) -> list:
    """Arrow IPC bytes -> always a Python list (even if 1 element)."""
    tbl = pa.ipc.open_stream(field['data']).read_all()
    return tbl.column(0).to_pylist()


def _matrix_table_to_numpy(tbl):
    """Arrow matrix table -> numpy ndarray, dropping flumen sentinel columns.

    R encodes matrix row labels as a sentinel string column '.rownames'
    (serialize.R matrix branch). It is a private R<->R convention; leaving it
    in leaks the labels as an extra data column AND upcasts the whole ndarray
    to object dtype (strings mixed with numbers). Drop it at the Arrow level so
    it is never materialised in pandas. numpy has no row labels, so the names
    are discarded here -- symmetric to the R decoder, which restores them as
    rownames(m)."""
    sentinels = ('.rownames', '.names')
    keep = [c for c in tbl.column_names if c not in sentinels]
    if len(keep) != tbl.num_columns:
        tbl = tbl.select(keep)
    return tbl.to_pandas(split_blocks=True, self_destruct=True).to_numpy()


# A 1-D numpy array travels as a 'vector', the path of a Python list, so that R
# receives an atomic vector. This schema-metadata key records that it WAS an
# ndarray, so that a Python consumer gets an ndarray of shape (n,) back rather
# than a list. R ignores schema metadata, and every hop forwards these bytes as
# they are
NDARRAY_1D_KEY = b'flumen_ndarray'


def _is_ndarray_1d(tbl) -> bool:
    return bool((tbl.schema.metadata or {}).get(NDARRAY_1D_KEY))


def deserialize_param(field: dict):
    """Arrow IPC -> natural Python type based on the original R type.

    Special case: data_type == 'raw' returns the bytes intact (no Arrow decoding).
    Used by shell tasks that emit stdout as bytes (no file output declared).
    """
    data_type = field.get('data_type', 'data.frame')

    # "raw" means "already-encoded payload, pass the bytes through". Inside an
    # aggregation the type carries the list marker ("wrapped:raw"), so match on
    # the base type: comparing the full string sent these bytes to the Arrow
    # reader, which failed with "Corrupted message".
    if data_type == 'raw' or data_type == 'wrapped:raw':
        return bytes(field['data'])

    tbl = pa.ipc.open_stream(field['data']).read_all()

    # An aggregated parameter arrives as N fields each marked 'wrapped:<base>'
    # (one element of the aggregation; wrapper.py assembles them into a list).
    # Decode each element to its base type's natural Python value -- otherwise
    # a non-data.frame element falls through to the to_pandas() default and
    # reaches the task as a DataFrame instead of e.g. a path string / ndarray.
    if data_type == 'wrapped:vector':
        if _is_ndarray_1d(tbl):
            return tbl.column(0).to_numpy()
        # 1-row table -> scalar (so a file-path aggregation becomes a flat
        # list[str]); N-row -> list (genuine multi-element vector elements).
        col = tbl.column(0)
        return col[0].as_py() if len(col) == 1 else col.to_pylist()
    if data_type == 'wrapped:matrix':
        return _matrix_table_to_numpy(tbl)

    if data_type == 'vector':
        if _is_ndarray_1d(tbl):
            return tbl.column(0).to_numpy()
        # 1-row table -> scalar, same rule as the 'wrapped:vector' branch above.
        # In R a scalar IS a length-1 vector, so an R task receives new_input(x = 3)
        # as a bare value; returning [3.0] here made the two languages disagree and
        # silently broke arithmetic (the ?new_python_task example x * 2 gave
        # [3.0, 3.0] instead of 6). Aggregations are unaffected: their elements
        # travel as 'wrapped:vector' and wrapper.py keeps the list shape for them
        # (and for any multi-field group), so an _aggr of one still arrives as a
        # 1-element list.
        col = tbl.column(0)
        return col[0].as_py() if len(col) == 1 else col.to_pylist()
    elif data_type == 'matrix':
        return _matrix_table_to_numpy(tbl)
    elif data_type == 'list':
        # Avoid expensive conversions and return native Python types directly
        return [col.to_pylist() for col in tbl.columns]
    else:
        df = tbl.to_pandas(split_blocks=True, self_destruct=True)
        # R encodes character row.names as the sentinel '.rownames' column
        # (serialize.R data.frame branch). Restore it as the pandas index --
        # symmetric to the R decoder (rownames(df) <- df[['.rownames']]) -- so
        # the labels land as the index instead of leaking as a data column
        # (which also upcasts a later df.to_numpy() to object dtype).
        if '.rownames' in df.columns:
            df.set_index('.rownames', inplace=True)
            df.index.name = None
        return df


def new_message(sender: str, msg_id: str, msg_type, data_type: list, data_names: list,
                data: list, is_arrow: list, chunk: int = 1, chunk_size: int = None,
                elem_name=None) -> bytes:
    """Create a new binary message with an Arrow IPC envelope.

    `elem_name` (optional) is a per-row vector of per-element names within a
    data_names group -- carries names from a Python dict / R named-list return
    across IPC so consumers can preserve them. Default empty strings."""
    n = len(data)
    chunks_data = []
    payload_sizes = []
    element_n_chunks = []
    element_tot_sizes = []
    n_chunks_final = 1

    for d in data:
        tot = len(d)
        n_ch = max(1, -(-tot // chunk_size)) if (chunk_size and tot > 0) else 1
        n_chunks_final = max(n_chunks_final, n_ch)
        element_n_chunks.append(n_ch)
        element_tot_sizes.append(tot)

        if chunk_size and tot > 0:
            start = (chunk - 1) * chunk_size
            end   = min(chunk * chunk_size, tot)
            chunk_d = d[start:end]
        else:
            chunk_d = d

        chunks_data.append(chunk_d)
        payload_sizes.append(len(chunk_d))

    tot_size = len(data[0]) if data else 0
    msg_type_arr = msg_type if isinstance(msg_type, list) else [msg_type] * n

    elem_name_arr = (list(elem_name)
                     if elem_name is not None and len(elem_name) == n
                     else [''] * n)
    header_tbl = pa.table({
        'sender':           pa.array([sender] * n),
        'msg_id':           pa.array([msg_id] * n),
        'msg_type':         pa.array(msg_type_arr),
        'data_type':        pa.array(data_type),
        'data_names':       pa.array(data_names),
        'is_arrow':         pa.array(is_arrow),
        'file_name':        pa.array([''] * n),
        'elem_name':        pa.array(elem_name_arr),
        'chunk':            pa.array([chunk] * n, type=pa.int32()),
        'n_chunks':         pa.array([n_chunks_final] * n, type=pa.int32()),
        'tot_size':         pa.array([tot_size] * n, type=pa.float64()),
        'element_n_chunks': pa.array(element_n_chunks,  type=pa.int32()),
        'element_tot_size': pa.array(element_tot_sizes, type=pa.float64()),
        'payload_size':     pa.array(payload_sizes,     type=pa.float64()),
    })

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, header_tbl.schema) as writer:
        writer.write_table(header_tbl)
        
    header_raw = sink.getvalue().to_pybytes()
    # Wire format: [4B header_len LE][header Arrow IPC][payload_1]...[payload_N].
    return struct.pack('<i', len(header_raw)) + header_raw + b''.join(chunks_data)


def _serialize_single(result, task_id: str) -> tuple:
    """Serialize a single Arrow-compatible object -> (bytes, data_type).

    Accepts: pandas DataFrame, pyarrow Table, numpy ndarray, list, and Python
    scalars (int/float/str/bool plus their numpy counterparts). Scalars are
    wrapped as 1-row Arrow vectors -- parity with R where a length-1 vector
    is the standard scalar form (`function() 1` returns c(1)). The receiver's
    deserialize_param 'wrapped:vector' branch unwraps len==1 back to a Python
    scalar, so the round-trip is symmetric.
    """
    if isinstance(result, pd.DataFrame):
        # A non-default pandas index must cross to R as row.names, NOT as a data
        # column. pa.Table.from_pandas(preserve_index=None) would physicalize a
        # non-RangeIndex as an `__index_level_0__` / index-name column that R's
        # data.frame decoder (serialize.R) surfaces as a spurious column while
        # the labels are lost. Mirror of the R->Python `.rownames` sentinel (H2):
        #   - default RangeIndex  -> dropped; R regenerates default row.names.
        #   - unique single index -> promoted to the `.rownames` sentinel column
        #     that serialize.R decodes back into row.names.
        #   - MultiIndex / non-unique / `.rownames` name clash -> not expressible
        #     as R row.names; reset to real column(s) instead of crashing.
        idx = result.index
        is_default_range = (isinstance(idx, pd.RangeIndex)
                            and idx.start == 0 and idx.step == 1)
        if is_default_range:
            pass
        elif (not isinstance(idx, pd.MultiIndex) and idx.is_unique
              and '.rownames' not in result.columns):
            result = result.copy()
            result.insert(0, '.rownames', idx.astype(str).to_numpy())
            result = result.reset_index(drop=True)
        else:
            result = result.reset_index()
        table, result_type = pa.Table.from_pandas(result, preserve_index=False), 'data.frame'
    elif isinstance(result, pa.Table):
        table, result_type = result, 'data.frame'
    elif isinstance(result, np.ndarray) and result.ndim == 1:
        # A 1-D array is a VECTOR, not a one-column matrix: pd.DataFrame() below
        # made it (n, 1), so R received an n x 1 matrix and Python an (n, 1)
        # array that broadcast silently (v + w gave n x n). It takes the path of
        # a list, labelled so that Python rebuilds the ndarray (NDARRAY_1D_KEY).
        table = pa.table({'x': pa.array(result)}, metadata={NDARRAY_1D_KEY: b'1'})
        result_type = 'vector'
    elif isinstance(result, np.ndarray):
        # A numpy ndarray is label-less. pd.DataFrame(result) would name the
        # columns with the integer RangeIndex ("0".."n-1"), which the R matrix
        # decoder surfaces as spurious colnames -- and R cannot tell them from a
        # user's deliberate "0".."n". Emit R's placeholder convention ("V1".."Vn")
        # so serialize.R's existing matrix decode strips them, yielding a bare
        # matrix. Mirror of the R->Python ".rownames" sentinel handling.
        df = pd.DataFrame(result)
        df.columns = ['V' + str(i + 1) for i in range(df.shape[1])]
        table, result_type = pa.Table.from_pandas(df), 'matrix'
    elif isinstance(result, list):
        table, result_type = pa.table({'x': pa.array(result)}), 'vector'
    elif isinstance(result, (bool, int, float, str,
                             np.bool_, np.integer, np.floating)):
        # Scalar -> 1-row Arrow vector. bool is named BEFORE int
        # because bool inherits from int in Python; the order in the tuple
        # itself doesn't matter (isinstance checks all members).
        table, result_type = pa.table({'x': pa.array([result])}), 'vector'
    elif isinstance(result, (bytes, bytearray)):
        # Binary payload: handed over VERBATIM, no Arrow wrapping -- the mirror
        # of R's raw vector, which travels as the user's bytes and is passed
        # through undecoded on arrival (R/messaging.R, objects.py's own 'raw'
        # branch in deserialize_param). Returning early is the point: an Arrow
        # envelope here would be exactly the qs2 envelope that corrupts the
        # R -> Python direction.
        # is_arrow travels FALSE for this row (wrapper.py), so the R consumer
        # takes its pass-through branch and rebuilds a raw vector.
        return bytes(result), 'raw'
    else:
        raise TypeError(
            f"[{task_id}] Unsupported type: {type(result).__name__}. "
            f"Use: pandas DataFrame, pyarrow Table, list, numpy ndarray, "
            f"bytes, or scalar (int/float/str/bool)."
        )

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)

    return sink.getvalue().to_pybytes(), result_type


def serialize_result(result, task_id: str, has_dyn_output: bool = False):
    """Python object -> (data, result_type, data_names).

    Always returns a 3-tuple. `data_names` is a list of strings for the dict
    (named multi-output) case, or None otherwise -- the caller passes it
    through to _send_result so the envelope's data_names field carries the
    names end-to-end. Parity with R's named-list return in wrapper.R, where
    `names(result)` is forwarded via data_names.

    Multi-output dispatch order:
      0. NAMED MULTI-OUTPUT (dict): each (key, value) pair becomes one named
         output instance -- mirrors R's `list(a = v1, b = v2)` with names
         preserved across IPC. Keys are str()-coerced (R names are character).
      1. EXPLICIT INTENT (parity with wrapper.R): the task declares an
         output with {{wc}} dynamic wildcard AND the user returns a list -->
         each list element becomes one output instance. Matches R's semantics
         where `list(v1, v2, v3)` with {{out}} produces 3 outputs regardless
         of element types (including plain scalars or lists).
      2. FALLBACK (back-compat): no dyn wildcard declared but the list contains
         a complex type (DataFrame/Table/ndarray) --> treat as multi-output.
         Preserves the original SUPPORTED_TYPES heuristic used by tests that
         predate the discriminator change.
    """
    if isinstance(result, dict):
        # Empty dict -> 0 outputs (mirrors R `list()` semantics for multi-output).
        items = list(result.items())
        parts = [_serialize_single(v, task_id) for _, v in items]
        names = [str(k) for k, _ in items]
        return SerializedResult(parts, 'multi', names)

    if isinstance(result, list) and len(result) > 0:
        if has_dyn_output or any(isinstance(item, SUPPORTED_TYPES) for item in result):
            parts = [_serialize_single(item, task_id) for item in result]
            return SerializedResult(parts, 'multi', None)

    data, dtype = _serialize_single(result, task_id)
    return SerializedResult(data, dtype, None)


def merge_chunks(chunk_dict: dict, n_chunks: int) -> list:
    """Reassemble partial chunks into the original format."""
    first_fields = chunk_dict[1]
    n_fields = len(first_fields)

    parts = [[] for _ in range(n_fields)]
    for c in range(1, n_chunks + 1):
        for i, f in enumerate(chunk_dict[c]):
            parts[i].append(f['data'])

    merged = [dict(f) for f in first_fields]
    for i in range(n_fields):
        merged[i]['data'] = b''.join(parts[i])
        # Mirror of R/messaging.R::merge_all_chunks: the reassembled payload
        # must be exactly as long as the sender declared. A lost or duplicated
        # chunk otherwise surfaces far away -- as a corrupt Arrow stream, or as
        # a silently truncated object. `file` is not skipped here as it is on
        # the R side: a Python wrapper never reassembles a file transfer, those
        # go to disk chunk by chunk on the supervisor.
        exp = merged[i].get('element_tot_size')
        if exp is not None and exp > 0 and len(merged[i]['data']) != exp:
            raise ValueError(
                f"flumen: element {i + 1} of the message from "
                f"{merged[i].get('sender', '?')!r} reassembled to "
                f"{len(merged[i]['data'])} bytes, the sender declared {exp} "
                f"({n_chunks} chunk(s) received). The transfer lost or "
                f"duplicated data; the payload is not usable.")

    return merged


def check_result_type(result, task_id: str):
    """
    Verify the result is serializable: must be one of the types accepted by
    _serialize_single, OR a list / dict whose elements/values are themselves
    serializable. dict enables named multi-output (parity with R named-list
    return); scalars enable `return 1`-style single outputs.
    """
    SCALAR_TYPES = (bool, int, float, str, np.bool_, np.integer, np.floating)
    ## bytes / bytearray are accepted here too: _serialize_single hands them
    ## over verbatim as the 'raw' type. Listing them only there is not enough
    ## -- this gate runs FIRST, so without them a bare `return b"..."` is
    ## rejected while the same bytes inside a list or dict go through.
    BINARY_TYPES = (bytes, bytearray)
    if not isinstance(result, (*SUPPORTED_TYPES, list, dict, *SCALAR_TYPES, *BINARY_TYPES)):
        raise TypeError(
            f"[{task_id}] Unsupported type: {type(result).__name__}. "
            f"Use: pandas DataFrame, pyarrow Table, list, dict, numpy ndarray, bytes, "
            f"or scalar (int/float/str/bool)."
        )
