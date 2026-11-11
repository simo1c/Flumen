#!/usr/bin/env python3
"""
wrapper.py — Python worker for Flumen.
Mirror of wrapper.R: NNG pair socket + Arrow IPC.

Usage: python3 wrapper.py <exe_id> <socket_path> <chunk_size>

API contract:
  - The main function must be the LAST top-level function defined in the
    user Python code. Helper functions must be defined before it.
  - The result must be: pandas DataFrame, pyarrow Table, list, numpy ndarray.
"""

import sys
import os
import re
import ast
import time
import textwrap

# A wrapper is SIGKILLed by its supervisor as soon as the result is delivered
# (R/supervisor_lib.R), and SIGKILL cannot be caught, so anything still in a buffer
# is never written. Python block-buffers stdout when it is not a terminal, and a
# wrapper never is, so every print() the user wrote would be lost silently. An R
# task has no such problem, so without this the same statement would behave
# differently in the two languages.
#
# Line buffering rather than full unbuffering flushes on every newline, which is
# what print() ends with, at a fraction of the syscalls. Guarded because
# reconfigure() is Python 3.7+ and no interpreter should fail to start over a
# diagnostic convenience.
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

# Ensure objects.py (sibling file in inst/scripts/) is importable regardless of CWD.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pynng
import pyarrow as pa
from objects import (decode_message, deserialize_field, deserialize_vector_field,
                     deserialize_param, new_message, serialize_result,
                     merge_chunks, check_result_type,
                     extract_multiple_wildcard_values, is_recursive_wildcard,
                     object_output_labels, N_FIXED_DISPATCH_FIELDS)


def _to_log(msg, task_id):
    """Append one timestamped line to this instance's log file, mirroring
    R/utils.R::to_log so a Python task's diagnostics land in the same place as an
    R or a shell task's.

    Kept in step by hand, like the rest of this file: wrapper.py cannot call the
    package's R helpers. The three pieces that must not drift are the directory
    (FLUMEN_LOG_DIR when run_workflow(log_dir=) published one, else "log/" under
    the working directory), the file name (the sender with everything outside
    [A-Za-z0-9._-] collapsed to "_", plus "_log.log") and the line shape
    ("<timestamp> - [<sender>] <msg>").

    Failure is swallowed: every caller has already written the same message to
    stderr, which reaches the master console, so a log that cannot be opened
    costs a copy and never the message itself."""
    try:
        log_dir = os.environ.get("FLUMEN_LOG_DIR", "") or os.path.join(os.getcwd(), "log")
        os.makedirs(log_dir, exist_ok=True)
        fname = re.sub(r"[^A-Za-z0-9._-]+", "_", str(task_id)) + "_log.log"
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(os.path.join(log_dir, fname), "a") as fh:
            fh.write(f"{stamp} - [{task_id}] {msg}\n")
    except OSError:
        pass


def _list_dir_entries(d, recursive):
    """Directory listing that mirrors R's list.files(). recursive=False ->
    top-level entries, files AND sub-dir names, as before.
    recursive=True -> files only, as paths RELATIVE to `d` (sub-dirs walked),
    matching list.files(recursive=TRUE). Used for whole-folder output globbing.

    DOTTED NAMES ARE LEFT OUT, and that is the part that has to match rather
    than merely look reasonable. R's list.files() defaults to
    all.files = FALSE, so it excludes any entry with a dotted component:
    `out/.hidden.txt`, `out/sub/.hidden.txt` and `out/.hiddendir/x.txt` are all
    absent, while `out/sub/x.txt` is present, so a dot directory is not even
    descended into. os.listdir and os.walk include all three. Without this
    filter the same declared dynamic output therefore collects a DIFFERENT set
    depending on whether the producer is written in R or in Python: an R
    producer drops `.hidden.txt` from a four-file gather while a Python producer
    keeps it, exit 0 and no warning either way. The policy chosen is R's, on the grounds that a pipeline output
    the user means to consume is not a hidden file."""
    if not os.path.isdir(d):
        return []
    if not recursive:
        return [e for e in os.listdir(d) if not e.startswith(".")]
    out = []
    for root, dirs, files in os.walk(d):
        ## In place, because os.walk reads this list to choose where to descend:
        ## replacing it is how a dot directory is pruned rather than skipped
        ## entry by entry.
        dirs[:] = [x for x in dirs if not x.startswith(".")]
        for fn in files:
            if fn.startswith("."):
                continue
            out.append(os.path.relpath(os.path.join(root, fn), d))
    return out


# Populated by main() so the top-level catch-all (see `if __name__ ...`) can
# notify the supervisor of an error in the wrapper itself even though s/task_id/etc.
# are locals of main().
_state = {}


def _arrow_bytes(tbl: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, tbl.schema) as w:
        w.write_table(tbl)
    return sink.getvalue().to_pybytes()


def _send_error_message(s, task_id: str, message: str, chunk_size: int,
                        stderr_str: str = "", status: int = -1) -> None:
    """Mirror of R-side send_error_message(): notify the supervisor of a
    user-code failure so it can propagate error_notify to the scheduler
    and avoid an indefinite hang.

    The supervisor decodes this via from_arrow_binary_to_R() into a
    one-row data.frame and reads $message / $stderr / $status fields.
    """
    err_tbl = pa.table({
        'task_id': pa.array([task_id],     type=pa.string()),
        'message': pa.array([message],     type=pa.string()),
        'stderr':  pa.array([stderr_str],  type=pa.string()),
        'status':  pa.array([status],      type=pa.int32()),
    })
    err_bytes = _arrow_bytes(err_tbl)
    err_msg = new_message(
        sender     = task_id,
        msg_id     = task_id,
        msg_type   = 'error',
        data_type  = ['data.frame'],
        data_names = [''],
        data       = [err_bytes],
        is_arrow   = [True],
        chunk      = 1,
        chunk_size = chunk_size,
    )
    s.send(err_msg)


def _find_main_func(user_code: str, task_id: str):
    """Return the name of the last top-level function defined in user_code."""
    tree = ast.parse(user_code)
    last_func_name = None
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            last_func_name = node.name
    if last_func_name is None:
        # No id in the text: task_id here is the exe_id, which names nothing the
        # user wrote, and the error envelope around this already
        # carries the task's own name. The stderr path below keeps its id, where
        # there is no envelope and correlation is all one has.
        raise NameError(
            "no function found in the task script. A Python task must define at "
            "least one top-level function; flumen calls the last one with the "
            "declared inputs."
        )
    return last_func_name


def _connect(socket_path: str, task_id: str):
    """Dial the supervisor's per-wrapper NNG pair socket and announce READY. On dial
    failure there is no socket to report on, so log to stderr and exit 1 (the supervisor
    detects the missing READY). Returns the open socket."""
    try:
        s = pynng.Pair0()
        s.dial(socket_path)
    except Exception as e:
        print(f"[{task_id}] Failed to dial supervisor socket '{socket_path}': {e}", file=sys.stderr, flush=True)
        sys.exit(1)
    s.send(new_message(
        sender    = task_id, msg_id = task_id, msg_type = 'ready',
        data_type = ['R_object'], data_names = [''],
        data      = [_arrow_bytes(pa.table({'x': pa.array([0], type=pa.int32())}))],
        is_arrow  = [True]))
    return s


def _send_result(s, task_id: str, chunk_size: int, result_data, result_type: str,
                 dyn_wildcard: str, output_names=None, output_par_names=None) -> None:
    """Build the result payload (the producer's serialized data + the dyn_wildcard /
    n_outputs trailer rows) and send it to the supervisor in chunks. `result_data` /
    `result_type` are the output of serialize_result(). `output_names` is the list of
    per-output names for the named multi-output case (dict return); when None, all
    names are empty strings (positional outputs, parity with R's default behaviour)."""
    if result_type == 'multi':
        # "wrapped:X" preserves the "was a list" marker for consistency with the R wrapper
        # (the R supervisor uses startsWith("wrapped") to avoid unwrapping list-returns).
        n_outputs        = len(result_data)
        all_result_bytes = [r[0] for r in result_data]
        all_data_types   = [f'wrapped:{r[1]}' for r in result_data]
    else:
        n_outputs        = 1
        all_result_bytes = [result_data]
        all_data_types   = [result_type]

    dyn_bytes  = _arrow_bytes(pa.table({'x': pa.array([dyn_wildcard], type=pa.string())}))
    nout_bytes = _arrow_bytes(pa.table({'x': pa.array([n_outputs],    type=pa.int32())}))

    all_data      = all_result_bytes + [dyn_bytes, nout_bytes]
    all_msg_types = ['result'] * n_outputs + ['dyn_wildcard', 'n_outputs']
    all_dtypes    = all_data_types + ['vector', 'vector']
    # data_names per row is the GROUPING slot: which declared OUTPUT the result
    # belongs to. It differs from elem_name in cardinality -- elem_name names
    # ONE element, a group name is shared by every result of the same output (a
    # dynamic file output matching N files sends N results under one name). One
    # entry per collected file, so it is as long as the results by construction.
    # The two trailer rows always carry empty names.
    all_names = (list(output_par_names) + ['', '']) if output_par_names is not None \
                else [''] * (n_outputs + 2)
    # Named outputs (dict return) populate the per-result elem_name; the two
    # trailer rows (dyn_wildcard, n_outputs) always carry empty names. When
    # output_names is None or its length doesn't match n_outputs, fall back to
    # all-empty names, so the envelope length cannot go out of step.
    if output_names is not None and len(output_names) == n_outputs:
        all_elem_names = list(output_names) + ['', '']
    else:
        all_elem_names = [''] * (n_outputs + 2)
    # Per-ROW, not a blanket True: a 'raw' payload travels verbatim and must be
    # flagged non-Arrow so the R consumer passes it through instead of handing
    # it to the Arrow reader. Every other type is Arrow-encoded, so the flag
    # stays True for them.
    def _is_raw_dtype(dt):
        return dt.rsplit(':', 1)[-1] == 'raw'
    all_is_arrow  = [not _is_raw_dtype(dt) for dt in all_data_types] + [True, True]

    max_size = max((len(d) for d in all_data), default=0)
    n_ch     = max(1, -(-max_size // chunk_size)) if chunk_size > 0 else 1
    for c in range(1, n_ch + 1):
        s.send(new_message(
            sender    = task_id, msg_id = task_id,
            msg_type  = all_msg_types, data_type = all_dtypes, data_names = all_names,
            data      = all_data, is_arrow = all_is_arrow,
            elem_name = all_elem_names,
            chunk = c, chunk_size = chunk_size))


def main():
    if len(sys.argv) < 4:
        print("Usage: python3 wrapper.py <exe_id> <socket_path> <chunk_size>")
        sys.exit(1)

    # 1. Positional argv, as the supervisor spawns it
    task_id       = sys.argv[1]
    socket_path = sys.argv[2]
    chunk_size  = int(float(sys.argv[3]))
    _state['task_id'] = task_id
    _state['chunk_size'] = chunk_size

    # 2. Dial the supervisor and announce READY
    s = _connect(socket_path, task_id)
    _state['s'] = s

    # 3. Chunk receive loop
    pending = {}
    task_done = False

    while not task_done:
        raw    = s.recv()
        fields = decode_message(raw)

        header   = fields[0]
        msg_id   = header['msg_id']
        chunk    = header['chunk']
        n_chunks = header['n_chunks']

        if msg_id not in pending:
            pending[msg_id] = {}
        pending[msg_id][chunk] = fields

        if len(pending[msg_id]) == n_chunks:
            merged = merge_chunks(pending.pop(msg_id), n_chunks)

            # 4. Decode the standard fields
            user_code        = textwrap.dedent(deserialize_field(merged[1]))
            raw_out_files    = deserialize_vector_field(merged[2])
            out_files        = [f for f in raw_out_files if f]
            is_output_par    = deserialize_vector_field(merged[3])
            dyn_wildcards    = deserialize_vector_field(merged[4])
            # Per-output auto_mkdir flag (merged[5]). Parallel vector to
            # raw_out_files; filter by the same "non-empty" mask used above
            # to stay aligned with out_files.
            raw_auto_mkdir   = deserialize_vector_field(merged[5])
            out_auto_mkdir   = [m for f, m in zip(raw_out_files, raw_auto_mkdir) if f]
            # Per-output declared NAME (merged[6]); parallel vector to
            # raw_out_files, built with it in R/object_func.R and filtered by
            # the same "non-empty" mask. Read, never inferred: which output a
            # produced file belongs to is part of the declaration.
            raw_out_par_names    = deserialize_vector_field(merged[6])
            out_par_names        = [n for f, n in zip(raw_out_files, raw_out_par_names) if f]
            # Declared OBJECT outputs by name (merged[7]), one entry each. It is
            # indexed on the declared object outputs, NOT on raw_out_files, so
            # it is deliberately not filtered by the mask used above: masking it
            # would shift it by however many file patterns the task declared.
            out_object_par_names = deserialize_vector_field(merged[7])

            # The FORM each parameter is delivered in (merged[8]): "bare", "list" or
            # "aggregate", one entry per declared parameter in slot order. The
            # scheduler declares it and the wrapper reads it to build the container.
            # The field layout is a contract: a message with fewer fixed fields than
            # this build expects comes from a supervisor of another build, and its
            # parameter fields start where this wrapper reads a fixed one.
            if len(merged) < N_FIXED_DISPATCH_FIELDS:
                raise RuntimeError(
                    f"the dispatch message carries {len(merged)} fixed field(s), this wrapper "
                    f"expects {N_FIXED_DISPATCH_FIELDS}. The supervisor and this wrapper come "
                    f"from different builds of flumen: reinstall the same package everywhere.")
            params_form = deserialize_vector_field(merged[8])

            # 4b. Pre-create parent dirs for outputs with auto_mkdir=True, matching
            # wrapper.R so the DSL flag means the same in R, shell and Python.
            # auto_mkdir=False leaves the directory alone, so a tool that creates its
            # own --id dir (cellranger) does not collide.
            for f, mk in zip(out_files, out_auto_mkdir):
                if mk:
                    f_clean = f.replace('`', '')
                    d = os.path.dirname(f_clean) or '.'
                    os.makedirs(d, exist_ok=True)

            # 5. Assemble par_list
            par_list     = {}
            dyn_wildcard = ''

            if len(merged) > N_FIXED_DISPATCH_FIELDS:
                groups        = {}
                ordered_names = []
                for f in merged[N_FIXED_DISPATCH_FIELDS:]:
                    name = f['data_names']
                    if name not in groups:
                        groups[name] = []
                        ordered_names.append(name)
                    groups[name].append(f)

                for j, name in enumerate(ordered_names):
                    current_fields = groups[name]
                    if j < len(is_output_par) and is_output_par[j]:
                        if j < len(dyn_wildcards) and dyn_wildcards[j] and not dyn_wildcard:
                            dyn_wildcard = deserialize_field(current_fields[0])
                    else:
                        dtype  = current_fields[0]['data_type']
                        values = [deserialize_param(f) for f in current_fields]

                        # elem_name carries per-element names from the supervisor, set
                        # when the producer returned a named multi-output (a Python
                        # dict or an R named list). The group is exposed as a dict
                        # {elem_name -> value or list of values} so a task can address
                        # it by name, but only when the slot holds a COLLECTION: the
                        # predicate the R consumer applies in
                        # R/serialize.R::aggregate_params, more than one constituent or
                        # an explicit gather ("wrapped", a list even at N=1).
                        #
                        # A single non-gathered constituent is delivered BARE, as in R.
                        # At N=1 the producer's name discriminates nothing, the slot
                        # being identified by the parameter name in new_input, and
                        # keying on it would make the parameter's SHAPE depend on
                        # whether the producer happened to name its return, which the
                        # consumer neither controls nor sees.
                        if len(params_form) != len(ordered_names):
                            raise RuntimeError(
                                f"the dispatch message declares {len(params_form)} parameter "
                                f"form(s) for {len(ordered_names)} parameter(s): the supervisor "
                                f"and this wrapper come from different builds of flumen")
                        # DECLARED container, read off the message.
                        is_collection = params_form[j] in ('list', 'aggregate')
                        if not is_collection and len(values) != 1:
                            raise RuntimeError(
                                f"parameter '{name}' is declared as one value but the message "
                                f"carries {len(values)} fields for it: the supervisor and this "
                                f"wrapper come from different builds of flumen")
                        elem_names = [f.get('elem_name', '') or ''
                                      for f in current_fields]
                        if is_collection and any(en for en in elem_names):
                            grouped = {}
                            # Keys already collapsed into a list of values. Read
                            # from here, not from isinstance(..., list): a VALUE
                            # may itself be a list, and was then taken for the
                            # group and extended ({'a': [1, 10, [2, 20]]}).
                            collapsed = set()
                            for en, v in zip(elem_names, values):
                                key = en if en else ''
                                if key in grouped:
                                    # collapse duplicates into a list
                                    if key in collapsed:
                                        grouped[key].append(v)
                                    else:
                                        grouped[key] = [grouped[key], v]
                                        collapsed.add(key)
                                else:
                                    grouped[key] = v
                            par_list[name] = grouped
                        else:
                            par_list[name] = values if is_collection else values[0]


            # 5a. A file the task declares it will write must not exist before the
            # task runs (mirror of
            # R/wrapper_lib.R::wrapper_refuse_preexisting_outputs). A static output
            # that already exists, or a dynamic pattern that already matches something
            # in its directory, is refused here, naming the file. With nothing
            # matching before the body, everything matching after is the task's own by
            # construction, so no snapshot of mtime and size is needed.
            _preexisting_err = None
            for f in out_files:
                f_clean = f.replace('`', '')
                d = os.path.dirname(f_clean) or '.'
                base = os.path.basename(f_clean)
                is_dyn = re.search(r'\{\{[^{}]+\}\}', base) is not None
                if is_dyn:
                    present = [os.path.join(d, m) for m in
                               extract_multiple_wildcard_values(
                                   base, _list_dir_entries(d, is_recursive_wildcard(f_clean)))['matches']]
                else:
                    present = [f_clean] if os.path.exists(f_clean) else []
                if not present:
                    continue
                shown = present[:5]
                if len(present) > 5:
                    shown = shown + [f"and {len(present) - 5} more"]
                _preexisting_err = (
                    f"instance '{task_id}' output '{f}' already exists: "
                    f"{', '.join(shown)}. Fix: remove it or write to another path.")
                break
            if _preexisting_err is not None:
                print(f"[WR_ERR:{task_id}] {_preexisting_err}", file=sys.stderr, flush=True)
                try:
                    _send_error_message(s, task_id, _preexisting_err, chunk_size)
                except Exception as send_err:
                    print(f"[WR_ERR:{task_id}] failed to notify supervisor: {send_err}",
                          file=sys.stderr, flush=True)
                try:
                    s.close()
                except Exception:
                    pass
                sys.exit(1)

            # 5b. The NAMES present in each output directory before the body,
            # kept only for the courtesy warning about by-products no declared
            # pattern matches (so it does not name the directory's unrelated
            # neighbours). Not used to decide what the task produced.
            out_dirs_snapshot = {}
            if out_files:
                for f in out_files:
                    f_clean = f.replace('`', '')
                    d = os.path.dirname(f_clean) or '.'
                    rec = is_recursive_wildcard(f_clean)
                    key = (d, rec)
                    if key in out_dirs_snapshot:
                        continue
                    out_dirs_snapshot[key] = set(_list_dir_entries(d, rec)) if os.path.isdir(d) else set()

            # 6. Compile and run the user function.
            # Wrap parse/exec/run/type-check under one try/except: any user-code
            # failure (SyntaxError, missing function, runtime exception, wrong
            # return type) must be reported to the supervisor; otherwise the
            # supervisor blocks waiting for a result that will never arrive.
            try:
                func_name     = _find_main_func(user_code, task_id)
                compiled_code = compile(user_code, filename="<user_code>", mode="exec")
                ns = {}
                exec(compiled_code, ns)
                func  = ns[func_name]
                # flumen guard: an input param still carrying a {{...}} template
                # means a dependency did not resolve (typically a new_*_aggr gather
                # over a dynamic-wildcard output that couldn't be matched to a
                # producer). Fail loud here instead of silently passing the literal
                # template into the user function.
                import re as _re
                def _strs(x):
                    if isinstance(x, str): return [x]
                    if isinstance(x, (list, tuple)): return [s for e in x for s in _strs(e)]
                    if isinstance(x, dict): return [s for e in x.values() for s in _strs(e)]
                    return []
                for _pn, _pv in par_list.items():
                    for _v in _strs(_pv):
                        if _re.search(r"\{\{[^}]+\}\}", _v):
                            raise ValueError(
                                "flumen: input '%s' was not resolved -- it still contains the "
                                "dependency template '%s'. A new_*_aggr gather over a dynamic "
                                "{{...}} output that could not be matched to a producer leaves "
                                "the template unsubstituted. Align the dep pattern with the "
                                "producer's output, or give that output a static name." % (_pn, _v))
                result = func(**par_list)

                # 7. Check type and serialize result. Skip the type check when
                # the task declares file outputs -- the user's return value is
                # ignored in that case (parity with wrapper.R, which
                # always overrides `result` with the enumerated file paths).
                if not out_files:
                    check_result_type(result, task_id)
            except BaseException as e:
                err_msg = f"{type(e).__name__}: {e}"
                print(f"[WR_ERR:{task_id}] user code error: {err_msg}",
                      file=sys.stderr, flush=True)
                try:
                    _send_error_message(s, task_id, err_msg, chunk_size)
                except Exception as send_err:
                    print(f"[WR_ERR:{task_id}] failed to notify supervisor: {send_err}",
                          file=sys.stderr, flush=True)
                try:
                    s.close()
                except Exception:
                    pass
                sys.exit(1)

            output_par_names = None
            if out_files:
                # 7b. File-output path (parity with wrapper.R): enumerate
                # files produced or modified during func() across declared output
                # patterns, then build a multi-output result of `file_path` cells
                # so the supervisor handles them exactly like R-produced files.
                files_new = []
                # Which declared output produced each file, parallel to
                # files_new. The loop holds it in out_par_names[_oi]; concatenating
                # the matches alone is what threw it away.
                files_owner = []
                # Per directory, what this run wrote and which declared DYNAMIC
                # patterns point at it. Collected inside the loop and judged
                # after it, because the question "did anything go uncollected"
                # needs what EVERY declared output matched: a task declaring two
                # patterns in one directory would otherwise report each one's
                # files as the other one's leftovers. Mirror of
                # R/fs.R::warn_unmatched_produced_files() -- keep the two in step.
                unmatched_watch = {}
                for _oi, f in enumerate(out_files):
                    _owner = out_par_names[_oi]
                    f_clean = f.replace('`', '')
                    d = os.path.dirname(f_clean) or '.'
                    base = os.path.basename(f_clean)
                    rec = is_recursive_wildcard(f_clean)
                    snap = out_dirs_snapshot.get((d, rec), set())
                    current_all = _list_dir_entries(d, rec)
                    # Everything the pattern matches now is the task's own:
                    # nothing matching existed before the body (step 5a). The
                    # names that appeared since the start feed only the warning
                    # about undeclared by-products below.
                    produced = [n for n in current_all if n not in snap]
                    file_matched = extract_multiple_wildcard_values(base, current_all)['matches']
                    # A recursive bundle claims its whole sub-tree, so nothing
                    # under it can be left out; a static output's neighbours are
                    # documented as ignored. Only the dynamic, non-recursive
                    # patterns can drop a file without saying so.
                    w = unmatched_watch.setdefault(
                        d, {'produced': set(), 'matched': set(), 'patterns': []})
                    # Matched names are relative to THIS output's directory, so
                    # they are recorded under it and nowhere else. A static
                    # output in the same directory contributes here too, which is
                    # what keeps its own file from being reported as a leftover.
                    w['matched'].update(file_matched)
                    if re.search(r'\{\{[^{}]+\}\}', base) and not rec:
                        w['patterns'].append(f)
                        w['produced'].update(
                            n for n in produced
                            if not os.path.isdir(os.path.join(d, n)))
                    if file_matched:
                        files_new.extend(os.path.join(d, m) for m in file_matched)
                        files_owner.extend([_owner] * len(file_matched))
                    elif re.search(r'\{\{[^{}]+\}\}', base):
                        # Dynamic output pattern declared but matched ZERO
                        # files: user code did not write expected output.
                        # Fail loud at the producer instead of propagating
                        # the literal pattern string downstream (cryptic
                        # consumer error tens of minutes later). Mirrors
                        # wrapper.R. Static literal outputs (no {{wc}}) take
                        # the fallback at the else branch instead.
                        err_msg = (
                            f"instance '{task_id}' declared dynamic output "
                            f"pattern '{f_clean}' but produced no matching "
                            f"files in directory '{d}'. Verify the tool "
                            f"actually wrote files matching the pattern "
                            f"(current working dir, path prefix, file "
                            f"extension, etc.)."
                        )
                        print(f"[WR_ERR:{task_id}] {err_msg}",
                              file=sys.stderr, flush=True)
                        try:
                            _send_error_message(s, task_id, err_msg, chunk_size)
                        except Exception as send_err:
                            print(f"[WR_ERR:{task_id}] failed to notify "
                                  f"supervisor: {send_err}",
                                  file=sys.stderr, flush=True)
                        try:
                            s.close()
                        except Exception:
                            pass
                        sys.exit(1)
                    elif not os.path.exists(f_clean):
                        # Static literal output not produced. Existence is
                        # enough: a file already at that path before the body
                        # was refused at step 5a, so one present now was written
                        # by this run. Mirror of R/fs.R::collect_declared_output.
                        err_msg = (
                            f"instance '{task_id}' declared output '{f_clean}' but "
                            f"the task did not produce it (no such file exists "
                            f"after the task ran). Verify the tool actually wrote "
                            f"it (current working dir, path prefix, file name)."
                        )
                        print(f"[WR_ERR:{task_id}] {err_msg}",
                              file=sys.stderr, flush=True)
                        try:
                            _send_error_message(s, task_id, err_msg, chunk_size)
                        except Exception as send_err:
                            print(f"[WR_ERR:{task_id}] failed to notify "
                                  f"supervisor: {send_err}",
                                  file=sys.stderr, flush=True)
                        try:
                            s.close()
                        except Exception:
                            pass
                        sys.exit(1)
                    else:
                        # Declared output produced but EMPTY: warn, never fail.
                        # Parity with wrapper.R / wrapper_shell.R
                        # (R/fs.R::warn_empty_declared_output) -- an empty
                        # result can be legitimate, but it is also the one
                        # failure that runs to completion with a plausible,
                        # biased answer, so it must not be silent. stderr is
                        # the channel that reaches the master console in every
                        # language.
                        try:
                            if os.path.getsize(f_clean) == 0:
                                _warn = (f"instance '{task_id}' declared output "
                                         f"'{f_clean}' and produced it EMPTY "
                                         f"(0 bytes). This is allowed -- an empty "
                                         f"result can be legitimate -- but "
                                         f"downstream tasks will read no data from it.")
                                print(f"[WR_WARN:{task_id}] {_warn}",
                                      file=sys.stderr, flush=True)
                                ## ...and to the per-instance log, like the R and the
                                ## shell wrappers. stderr reaches the console of the
                                ## session that launched the run; only the log file
                                ## survives it, and it is the copy a user reads when
                                ## looking into a finished run.
                                _to_log(f"[WR_WARN] {_warn}", task_id)
                        except OSError:
                            pass
                        # Literal pattern (no {{wc}}): use the declared path
                        # as-is, matching wrapper.R's fallback.
                        files_new.append(f_clean)
                        files_owner.append(_owner)

                # Files this run wrote into a dynamic output's directory that no
                # declared pattern matches: nobody collects them. Match five of six
                # and the sixth is dropped in silence, exit 0, a consumer short by one
                # sample, not a word in any channel. A warning and not an error,
                # because by-products beside an output are ordinary (a .bai next to a
                # .bam): the engine cannot know six were meant rather than five, only
                # which files it is leaving behind.
                for d, w in unmatched_watch.items():
                    if not w['patterns']:
                        continue
                    extra = sorted(w['produced'] - w['matched'])
                    if not extra:
                        continue
                    shown = extra[:5]
                    if len(extra) > 5:
                        shown = shown + [f"and {len(extra) - 5} more"]
                    _warn = (f"instance '{task_id}' wrote {len(extra)} file(s) into "
                             f"'{d}' that no declared output pattern matches, so "
                             f"they are NOT delivered downstream: "
                             f"{', '.join(shown)}. Declared here: "
                             f"{', '.join(repr(p) for p in w['patterns'])}. "
                             f"By-products are expected and this is only a "
                             f"warning, but a file meant as an output whose name "
                             f"does not fit the pattern would otherwise vanish "
                             f"without a word.")
                    print(f"[WR_WARN:{task_id}] {_warn}",
                          file=sys.stderr, flush=True)
                    _to_log(f"[WR_WARN] {_warn}", task_id)

                # Manually build the 'multi' result_data list so each file path
                # becomes one output cell tagged 'file_path' -- _send_result then
                # produces N fields with data_type 'wrapped:file_path', exactly
                # like the R wrapper does via wrapper_send_result.
                result_data = []
                for p in files_new:
                    tbl  = pa.table({'x': pa.array([p], type=pa.string())})
                    result_data.append((_arrow_bytes(tbl), 'file_path'))
                result_type = 'multi'
                output_par_names = files_owner
            else:
                # has_dyn_output mirrors wrapper.R's `out_dyn_par` (line 113):
                # any declared OUTPUT param that also carries a {{wc}} dynamic
                # wildcard. When true and the user returns a list, each
                # element becomes one output instance (multi-output) --
                # matches R's list-expansion semantics for {{wc}} outputs.
                has_dyn_output = any(
                    bool(d) and bool(o)
                    for d, o in zip(dyn_wildcards, is_output_par)
                )
                # serialize_result returns a SerializedResult namedtuple.
                # .elem_names is populated only for the dict-return case;
                # None otherwise. _send_result forwards it to the envelope's
                # data_names field for named multi-output parity with R's
                # `list(a=..., b=...)` named-list return.
                ser = serialize_result(result, task_id, has_dyn_output)
                result_data, result_type, output_names = (
                    ser.data, ser.data_type, ser.elem_names)

                # Which declared output each result belongs to. Same rule as the
                # R and shell wrappers (objects.py::object_output_labels), read
                # from the declaration rather than inferred from the return:
                # under a dynamic object output the dict keys are the wildcard
                # values, not the output's name.
                n_res = len(result_data) if result_type == 'multi' else 1
                output_par_names = object_output_labels(out_object_par_names,
                                                        n_res, output_names)


                # A dynamic OBJECT output that produced nothing fails loud at the
                # producer, mirroring the dynamic-FILE guard above: an empty 'multi'
                # would send n_outputs=0, mis-expanding downstream, or die in message
                # assembly, and a consumer that declared a dependency on it cannot run
                # meaningfully.
                #
                # An empty LIST needs its own arm. serialize_result() takes the multi
                # branch only for len(result) > 0 (objects.py), so [] never becomes
                # 'multi' and the test above can never fire for it, while {} does
                # become 'multi' with zero parts, caught by the first arm.
                #
                # Under a declared {{wc}} output a list means "these are my N
                # outputs", so an empty one means zero, the error case, as R treats
                # list(). Python has no separate empty-atomic literal, so "one empty
                # output" is np.array([]), "" or an empty DataFrame, all of which
                # still pass.
                #
                # An empty DICT is refused under a STATIC output too: a dict is a
                # named multi-output, one per key, so {} is zero outputs. The envelope
                # then carries no result at all, the supervisor takes its trailer for
                # an unknown message, and the run waits for ever. An empty list under
                # a static output stays one empty value.
                empty_multi = result_type == 'multi' and len(result_data) == 0
                empty_list  = isinstance(result, list) and len(result) == 0
                if empty_multi or (has_dyn_output and empty_list):
                    if has_dyn_output:
                        err_msg = (f"instance '{task_id}' declared a dynamic object output "
                                   f"({{{{...}}}}) but produced no output objects (empty "
                                   f"result). Verify the task returns at least one element.")
                    else:
                        err_msg = (f"instance '{task_id}' returned an empty dict: a dict is one "
                                   f"named output per key, so it produced no output at all. "
                                   f"Return the value itself, or a dict naming every "
                                   f"declared output.")
                    print(f"[WR_ERR:{task_id}] {err_msg}", file=sys.stderr, flush=True)
                    try:
                        _send_error_message(s, task_id, err_msg, chunk_size)
                    except Exception:
                        pass
                    try:
                        s.close()
                    except Exception:
                        pass
                    sys.exit(1)

            _send_result(s, task_id, chunk_size, result_data, result_type,
                         dyn_wildcard,
                         output_names=output_names if not out_files else None,
                         output_par_names=output_par_names)

            task_done = True

    # Idle loop (like wrapper.R: while(T){})
    while True:
        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Catch-all for errors in the wrapper itself (chunk reassembly,
        # field deserialization, result serialization, send) -- anything outside
        # the user-code try/except in main(). Notify the supervisor so the
        # workflow aborts cleanly instead of hanging on a result that never comes.
        err_msg = f"wrapper error: {type(e).__name__}: {e}"
        print(f"[WR_ERR] {err_msg}", file=sys.stderr, flush=True)
        _s = _state.get('s'); _tid = _state.get('task_id'); _cs = _state.get('chunk_size')
        if _s is not None and _tid is not None and _cs is not None:
            try:
                _send_error_message(_s, _tid, err_msg, _cs)
            except Exception:
                pass
            try:
                _s.close()
            except Exception:
                pass
        sys.exit(1)
