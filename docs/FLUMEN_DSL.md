# Flumen DSL Reference

This is the reference manual for writing Flumen workflows. Signatures derive from the Flumen source code. Hosts, IPs, usernames and paths are reported as generic placeholders. Replace them with your own when running. A construct written with parentheses, `new_workflow()`, denotes the call, while the same name without them, `new_input`, denotes the construct.

## 1. Core concepts

A Flumen workflow is composed of five core elements:

1.  Wildcard (section 3.1): a variable that instantiates the same task over many inputs (`new_wildcard`, `new_wildcard_list`).
2.  Task (section 3.2): a single R / Python / shell operation (`new_R_task`, `new_python_task`, `new_shell_task`), declared with `new_input`, `new_output`, `new_dep`, `new_file*`, `new_object*` (sections 3.2.2 and 3.2.3).
3.  Workflow (section 3.3): a DAG of tasks, validated as connected and acyclic (`new_workflow(workflow_id, desc, task_list, wildcard_list)`). At this point the wildcards are expanded into concrete instances and any build-time error is raised.
4.  Worker (section 3.4): an R process that runs tasks, either local (IPC) or remote (TCP) (`new_worker`), plus a launcher if the worker is remote (section 6.1).
5.  Execution (section 3.5): `run_workflow(workflow, worker_list, chunk_size, ...)`.

### 1.1 Fundamental rule: `{x}` vs `{{x}}`

| Syntax | Meaning | Resolved by | Where |
|------------------|------------------|------------------|------------------|
| `{name}` (single brace) inside a path/value | Static wildcard | Workflow construction (values from `new_wildcard_list`) | Inside a `new_file*` / `new_object*` argument, where it must be wrapped (section 5.1) |
| `{{name}}` (double brace) inside a path/value | Dynamic output | Task runtime (the user function produces the key) | Arguments of `new_file*` / `new_object*` |
| `{name}` inside a `new_script()` body (shell) | Parameter placeholder | Task process at runtime (substituted with `new_input` / `new_output` value) | Shell scripts only |

**Rule (wrapping)**: Every wildcard reference, static `{x}` as well as dynamic `{{x}}`, must be inside a Flumen constructor (`new_object*` / `new_file*`). A simple wildcard passed as a plain `new_input()` / `new_output()` value (e.g. `new_input(sample = "{sample}")`) is rejected by `new_workflow()` for all task types, shell included, with no exception.

**Disambiguation**: the same `{x}` syntax has different meanings depending on location (inside a path/value vs inside a shell-script body). The `{name}` placeholders inside a shell `new_script()` body withouth `new_object*` / `new_file*` constructors are correct.

**Constraint**: only one `{{name}}` per parameter, and each `{{name}}` must be unique across a task's parameters. Validation rejects both multiple dynamic wildcards in one path and reuse of the same `{{name}}` (for example `{{out}}`) on two parameters of the same task, whether an input dep and an output or two inputs. Rename one occurrence, for instance the output `{{out}}` to `{{out2}}`.

## 2. Quick-start template (copy/paste)

``` r
library(flumen)

w_list <- new_wildcard_list(sample = c("s1", "s2", "s3"))

A <- new_R_task(
  task_id = "A",
  input   = new_input(x = new_object_exp("{sample}")),
  output  = new_output(out = new_object("{{out}}")),
  exec    = new_script('
    f <- function(x) paste("processed", x)   # one output -> return it directly
  ')
)

B <- new_R_task(
  task_id = "B",
  input   = new_input(
    xs = new_dep(task_id = "A", value = new_object_aggr("{{out}}"))
  ),
  output  = new_output(final = new_object("{{final}}")),
  exec    = new_script('
    function(xs){
      g <- paste(unlist(xs), collapse = "; ")
      saveRDS(g, "test.Rds")
    }
  ')
)

workflow <- new_workflow(
  workflow_id   = "demo",
  desc          = "Expand A over {sample}, aggregate in B",
  task_list     = list(A, B),
  wildcard_list = w_list
)

run_workflow(
  workflow,
  worker_list = list(new_worker(name = "w1", n_concurrent_task = 1)),
  chunk_size  = 1e8
)
```

This template covers many common patterns: an R task with wildcard expansion and a second R task with an aggregate dependency on a single local worker.

## 3. Building blocks

### 3.1 Wildcards

``` r
new_wildcard(name, values)        # R/wildcard_dsl.R
new_wildcard_list(...)            # R/wildcard_dsl.R
```

-   `new_wildcard("sample", c("a","b","c"))`: atomic character values.

-   `new_wildcard("dataset", values = list(ob1 = data.frame(...)))`: values may be R objects (data.frame, list and so on) keyed by name. The names are required, and refused at build time when missing: an object has no natural key and a position is not an identity. Atomic values need no names because there the value is its own key.

-   `new_wildcard_list(...)` accepts:

    -   `wildcard_object` instances from `new_wildcard()` directly,
    -   named aliases: `sample = c("a","b")`, `ref = "ref1"`.

-   Wildcard names must be unique within the list.

-   The value or the key is decided by the pattern. When a wildcard's values are R objects keyed by name, a parameter whose text is exactly the token receives the object, while a parameter with any other text around the token receives the key as a character string. Expansion first substitutes the key into the pattern, so `"{data}"` becomes `"df1"` and `"id_{data}"` becomes `"id_df1"`. The result is then promoted to the object only when the original text was the token alone. Two consequences follow. A file pattern always yields a string, since a path is text. And an atomic wildcard such as `sample = c("s1","s2")` has no object to promote, so both forms yield a string.

    When a wildcard's values are objects, a function that requests `{data}` receives, for example, the data frame and nothing that names it, so the instance cannot determine which dataset it has. Asking for `id_{data}` in a second input gives the key as a string, `"id_df1"`. Declare both and build the output name from the key:

    ``` r
    add <- new_R_task(
      task_id = "add",
      input   = new_input(df = new_object_exp("{data}"),      # the data frame
                          id = new_object_exp("id_{data}")),  # "id_df1", the key
      output  = new_output(out = new_file("id_{data}.Rds")),
      exec    = new_script('
        f <- function(df, id) saveRDS(df, paste0(id, ".Rds"))
      ')
    )
    ```

    The two parameters are bound to the same instance by construction, not by arrival order: each instance carries its own wildcard binding, and a consumer instance depends on the producer instance that shares that binding. The guarantee holds for this one-to-one shape and does not carry over to an aggregation, where a single consumer has many parents and identity must move together with the data (section 7.2.1).

-   The value of a wildcard is substituted literally into paths and into the arguments a task receives. Two cautions apply, both about identity rather than about characters. A value that is a prefix of another value (`A` alongside `A_1`) is handled differently depending on where it is used: a per-sample input filter resolves it by longest match (section 5.3.1), while two dynamic output patterns that can match the same file names are refused at build time (section 8). Two values differing only by case (`s1` alongside `S1`) are rejected at build time when they resolve to case-twin output paths, on every platform, because on a case-folding filesystem they would be one file and one sample would silently overwrite the other. A value (or a key) equal to the wildcard's own name, simple or in a pattern form (`cond`, `{cond}`, `{{cond}}` for a wildcard named `cond`), is rejected at build time. Rename the value or the wildcard.

### 3.2 Tasks

``` r
new_R_task(task_id, input = NULL, output = NULL, exec, requires = NULL)        # R/task.R
new_python_task(task_id, input = NULL, output = NULL, exec, requires = NULL)   # R/task.R
new_shell_task(task_id, input = NULL, output = NULL, exec, requires = NULL)    # R/task.R
```

-   `task_id`: unique non-empty string.
-   `input` / `output`: objects from `new_input(...)` / `new_output(...)`, or `NULL` (but at least one of the two must be non-empty).
-   `exec`:
    -   `new_R_task()` / `new_python_task()` accepts only `new_script(...)` (class `script`).
    -   `new_shell_task` accepts `new_script(...)` or `new_cmd(...)`.
-   `requires`: optional named list with fields `cpu` (int ≥0), `mem` (string `"32G"`, `"128GB"`, `"16384M"`, `"1.5T"`), `gpu` (int ≥0), and `tags` (character vector of capability labels, matched by set membership, so the worker must advertise every requested tag). `NULL` = no constraint, any worker eligible.

**Errors**:

-   `"new_R_task(): 'exec' must be created with new_script()."`: a simple string was passed instead of `new_script(...)`.
-   `"new_task(): the task declares neither input nor output."`: both `input` and `output` are `NULL`.
-   `"new_task(): 'task_id' is required and must be a single non-empty string."`.
-   `"'requires': unknown field(s) 'foo'; allowed: 'cpu', 'mem', 'gpu', 'tags'."`.

#### 3.2.1 Script / Cmd

``` r
new_script(code = NULL, file = NULL)   # R/task.R
new_cmd(code)                          # R/task.R
```

-   `new_script` accepts `code` or `file` (mutually exclusive). When `file` is given, contents are read at call time.
-   For Python: `new_python_task` strips common leading indentation. You can freely indent Python code inside the R string.
-   `new_cmd` tags a single executable name (e.g. `"fastqc"`, `"bowtie2-build"`). Inputs/outputs are passed as CLI arguments. No shell is invoked.

#### 3.2.2 Input / Output

``` r
new_input(...)                       # R/fs.R — named or unnamed args
new_output(...)                      # R/fs.R — named args (used for matching)
pre_staged(x)                        # R/fs.R — wrap new_file()/new_file_exp() -> read in place on worker
```

The names of `new_input` arguments become parameter names in the user function (R/Python) or placeholder names `{name}` (shell). Example:

``` r
new_input(threads = 8, reads = new_file("..."))
# -> in shell script: {threads}, {reads} are substituted
# -> in R/Python: function arguments must be named `threads` and `reads`
```

For shell tasks, unnamed inputs are allowed (passed positionally to the command), and names can be CLI flags like `"--threads"` or `"-o"`.

#### 3.2.3 File / Object / Dep

``` r
new_file(file_path, auto_mkdir = TRUE)               # R/fs.R
new_file_exp(object, auto_mkdir = TRUE)              # R/fs.R — wildcard expansion
new_file_aggr(object, by = NULL, auto_mkdir = TRUE)  # R/fs.R — aggregation

new_object(object = NULL)                            # R/fs.R — auto-generates UUID if NULL
new_object_exp(object = NULL)                        # R/fs.R — expansion
new_object_aggr(object = NULL, by = NULL)            # R/fs.R — aggregation

new_dep(task_id, ...)                                # R/fs.R — dependency on another task's output
                                                     # one value: new_dep("A", new_object("{{x}}")) or value = ...
                                                     # several values are REJECTED (section 5.4)
```

**Disambiguation**: the first argument is called `file_path` in `new_file()` and `object` in the other five constructors, and in `new_file_exp()` and `new_file_aggr()` that argument named `object` holds a path pattern, not an object. What makes a parameter a file or an object is the constructor, never the argument's name.

-   `new_file(path)` tags a path as a file. `path` may contain `{wc}` (static wildcard) or `{{name}}` (dynamic output). `path` can also be a character vector such as `new_file(c("a.fa", "b.fa", "c.fa"))`, and Flumen treats every entry as a separate file in the bundle and delivers the resolved list to the consumer. This form is for inputs and outputs only, never inside a `new_dep()`, where it is rejected at build time (section 5.4). Use it for explicit reference bundles whose names are known at build time (see section 5.5 "Multi-file output patterns", scenario F4), and depend on a producer's files with `new_file_aggr("dir/{{any}}")`, which is also the form to use when the names are decided at runtime. Each path becomes a separate transfer slot at dispatch.

    ``` r
    ref_paths <- c("data/ref_x.fa", "data/ref_y.fa", "data/ref_z.fa")

    consume <- new_R_task(
      task_id = "consume",
      input   = new_input(
        reffiles = new_file(ref_paths)        # vector -> all three transferred as a bundle
      ),
      output  = new_output(res = new_object("{{out}}")),
      exec    = new_script('
        consume_ref <- function(reffiles) {
          paths <- if (is.list(reffiles)) unlist(reffiles, use.names = FALSE) else reffiles
          data.frame(n_files = length(paths))
        }
      ')
    )
    ```

-   `auto_mkdir` (default `TRUE`): pre-create the parent directory of a declared output before the body runs, with the same meaning in all three languages. The flag is carried per output, so two outputs of one task can differ. Set it to `FALSE` when the path is owned by an external tool that creates its own working directory and refuses to overwrite (e.g., `cellranger`). It governs outputs only, on a file used as an input it is accepted and has no effect.

-   Transfer vs pre-staged. By default the master reads the bytes of a declared input file at dispatch and transfers them to the worker. To mark a file as pre-staged (already present on a specific worker so that the master does not look for it), wrap the file input in `pre_staged()`: `new_input(config = pre_staged(new_file("/on/worker/cfg.txt")))`. `pre_staged()` accepts only a `new_file()` or `new_file_exp()` value. Wrapping a `new_file_aggr()`, `new_object()`, `new_dep()` or a literal value returns an error. A file declared pre-staged but missing on the worker raises an error before the wrapper is launched.

    ``` r
    A <- new_R_task(
      task_id = "A",
      input   = new_input(
        config = pre_staged(new_file("/already/on/worker/cfg.txt"))
      ),
      output  = new_output(out = new_object("{{out_A}}")),
      exec    = new_script('f <- function(config) config')
    )
    ```

-   Shared filesystem vs pre-staged file. `pre_staged()` marks a file already present on one worker. A mount visible to every worker (e.g., NFS) is instead declared once at launch via `run_workflow(shared_path = ...)` (see section Execution): any file whose absolute path lies under a declared root is read by the consumer from that root without touching the task definition.

-   `new_object()` with no argument auto-generates a UUID placeholder. It is useful as an anonymous quick output (`new_output(out = new_object())`). It is equivalent to `new_object("{{x}}")`.

-   `_exp` (expansion): each wildcard value spawns one task instance. Forbidden in `new_output(...)`. Outputs use plain `new_object()` / `new_file()`: expansion follows the input automatically and aggregation happens on the consumer side. Validation rejects either constructor in an output. An `_aggr` that uses a dynamic wildcard must be placed inside `new_dep(value = ...)`, since the dynamic name has meaning only relative to the producer that emitted it. An `_aggr` over a static wildcard may stand alone in `new_input(...)`, as a fan-in over already-existing files (`new_file_aggr("d/{s}.txt")`).

-   `_aggr` (aggregation): all upstream instances are collected into one list passed to a single consumer. It is not allowed in `new_output(...).`

-   Which wildcard an `_aggr` may group by depends on the type. Object aggregation groups by a dynamic name only: `new_object_aggr("{{name}}")`, matching the producer's `new_object("{{name}}")`. A static pattern there (`new_object_aggr("{sample}")`) matches no producer at runtime and is rejected at build time (see section 8). File aggregation accepts either: `new_file_aggr("dir/{wc}.ext")` groups by a static wildcard and `new_file_aggr("dir/{{name}}.ext")` by a dynamic one. Both match the corresponding pattern `new_file()` output of the producer.

-   Choosing between object and file aggregation. `new_object_aggr()` hands the function N live objects, so the consuming worker must hold the whole aggregate in RAM, first in the supervisor, which assembles it, and again in the wrapper, which materialises it as R objects to call the function. `new_file_aggr()` hands it N paths instead: what accumulates is only the path strings, and the data stays on disk until the function reads it. Aggregating objects is bounded by the consumer's RAM, aggregating files by its disk. See section 5.3.3.

-   **Pitfall**: indexing an object aggregation by position (`ms[[1]]`) attributes a result to the incorrect instance with no error and a successful run, so the values are correct and only the identity of the sample is not. The list is in completion order, which depends on how long each instance took, and is neither stable across runs nor predictable from the declaration. `names()` does not prevent it, being `NULL` when the producer declares a single dynamic output and the output parameter's own name repeated (`"a","a","a"`) when it declares several. To keep the identity, have the producer return a named value, since those names move together with the values and make `ms[["s1"]]` safe. A file aggregation is not exposed. Worked example in section 7.2.1.

-   `_aggr(..., by = "wc")`: partial aggregation, producing one consumer instance per combination of the `by` wildcards (`GROUP BY` semantics). Every other wildcard is aggregated into the list each instance receives.

-   `new_dep(task_id, ...)`: declares a dependency on `task_id`'s output. Write the parent's pattern after the task id, as in `new_dep("A", new_object("{{x}}"))`. A dependency names one output: passing more than one pattern is rejected, and so is a single pattern carrying several paths, such as `new_file(c("a.txt", "b.txt"))` (section 5.4).

### 3.3 Workflow

``` r
new_workflow(workflow_id, desc, task_list, wildcard_list = NULL)   # R/workflow.R
```

-   `workflow_id`: identifier for the workflow, a single non-empty string. It names the run, not its tasks, which carry their own `task_id`.
-   `desc`: free-text description. Required.
-   `task_list`: `list(...)` of task objects.
-   `wildcard_list`: output of `new_wildcard_list()` or `NULL`.
-   Validation order:
    1.  Task templates (pre-expansion): unique `task_id`s, valid `exec`, `new_dep` references exist, etc.
    2.  DAG: must be connected and acyclic.
    3.  Wildcard expansion: clones each task per combination of wildcard values, naming the clones as in section 9.1.

**Errors**:

-   `"the DAG is disconnected"`: the task graph is not connected. It is raised on an unconnected task, and equally on two chains that are each complete but never reference each other.
-   `"new_workflow(): dependency cycle in task_list, involving task_ids: '<id>', '<id>'."`.
-   Warning when a transferred (not `pre_staged()`) literal path does not exist on the master. `pre_staged()` inputs are not checked.

### 3.4 Worker

``` r
new_worker(name = NULL,
           n_concurrent_task = 1,
           self_host        = NULL,        # remote workers only (with launcher)
           ports            = 61100:61199, # this worker's P2P TCP port range
           cache_ram_limit  = 4096L,       # MB
           cache_disk_limit = 51200L,      # MB
           cache_dir        = NULL,
           R_cmd            = "Rscript",
           python_cmd       = "python3",
           shell_cmd        = "bash",
           root_path        = NULL,
           launcher         = NULL,        # R/worker.R
           resources        = NULL)        # local workers only
```

-   `name` required (unique).

-   `n_concurrent_task`: how many task instances this worker runs at the same time, one process each. It is per worker and not a total for the workflow: three workers at `n_concurrent_task = 2` run six tasks concurrently. Default 1, which serialises that worker.

-   `self_host` is forbidden when `launcher = NULL` (local workers inherit the master's host).

-   `ports`: the contiguous TCP port range this worker may use for peer-to-peer transfer. `ports[1]` is the data-request listen socket and `ports[2:N]` are the transfer pool. Default `61100:61199`. In an all-local workflow it has no effect, since peer transfer uses IPC. The sizing and overlap rules are in section 6.2.

-   `cache_ram_limit`: megabytes of RAM this worker's supervisor may hold in its result cache before spilling, default `4096`. The cache keeps the outputs a task returns so that a co-located dependent, or a peer that requests them, is served without re-running the producer. The RAM slot is evicted least-recently-used (LRU, and the evicted entry is written to `cache_dir` rather than discarded. Raising it does not raise the ceiling on an object aggregation, which is bounded by the consumer's RAM (section 5.3.3).

-   `cache_disk_limit`: megabytes of disk the spill tier may occupy, default `51200`. Unlike the RAM limit this one is hard: an entry that does not fit stops the worker with `Disk cache limit exceeded (<N> MB). Increase cache_disk_limit in new_worker().`

-   `cache_dir`: where the spill tier resides, under `<cache_dir>/result_cache/`. `NULL`, the default, uses `tempdir()` on the worker's own machine. The index of what was spilled is held in memory by the process that wrote it, so a spill file is never read back by a later run.

-   `R_cmd`, `python_cmd`, `shell_cmd`: the executables this worker uses to launch task bodies, defaulting to `Rscript`, `python3` and `bash`. The mapping is not one per language. An R task is launched with `R_cmd` and a Python task with `python_cmd`, while a shell task is launched with `R_cmd` as well, because the shell wrapper is itself an R script, and `shell_cmd` is the shell that wrapper hands the body to. Each is looked up on the worker's `PATH` unless given as an absolute path, and must be present on every node that may run the corresponding task type (section 4.1.4). A value carries an executable name only, never arguments.

-   `root_path`: the base directory under which this worker places the files it receives from a peer. `NULL`, the default, uses the worker's current directory. Files sent by the master are transferred in the working directory instead, so the two must agree: `ssh_launcher()` arranges this by defaulting its `cwd` to `root_path`, while a hand-started or custom-launched worker has to be started there (section 6.4).

-   `launcher`:

    -   `NULL`: a local worker, spawned by the scheduler itself.
    -   `ssh_launcher(...)` (section 6.1), or any object carrying a class for which a `launch_worker` method is defined. The method's signature is `launch_worker(config, worker)`.
    -   A plain function of one argument, called with the worker object. This is the form the localhost pattern of section 6.5 uses.

    Any other value is rejected by `new_worker()`.

-   `resources`: local workers only (`launcher = NULL`). Declares what the local worker offers, matched against what a task requests in `requires`. Same schema as `ssh_launcher(resources = ...)`, including `tags` capability labels. For launched or remote workers, declare resources on the launcher instead, which is authoritative. Setting `resources` on both the worker and its launcher is an error. The common use is pinning a task to the local machine through a tag, `new_worker(resources = list(tags = "mac"))` matched by `requires = list(tags = "mac")`.

#### 3.4.1 Resource pinning

``` r
# GPU-required task
task_gpu <- new_R_task(
  task_id  = "ml_train",
  input    = new_input(x = 1),
  output   = new_output(out = new_object("{{out}}")),
  exec     = new_script("f <- function(x) x"),
  requires = list(cpu = 16, mem = "64G", gpu = 1)
)

# GPU-capable worker
w_gpu <- new_worker(
  name     = "gpu1",
  launcher = ssh_launcher(
    host_user = "<user>@<gpu_host>",
    resources = list(cpu = 16, mem = "128G", gpu = 1)
  )
)

# Insufficient worker
w_cpu <- new_worker(
  name     = "cpu1",
  launcher = ssh_launcher(
    host_user = "<user>@<cpu_host>",
    resources = list(cpu = 8, mem = "32G")
  )
)

# Match: task_gpu lands on w_gpu (cpu>=16, mem>=64G, gpu>=1).
# w_cpu fails: missing gpu and not enough cpu.
# Pool with no eligible worker -> run_workflow stops fast with
#   "task '<id>' requires resources that no worker in worker_list can satisfy."
```

`mem` values are parsed into bytes and compared. A value is a number with `K`, `M`, `G` or `T`, optionally followed by `B`, and fractions are accepted (`"1.5T"`); a number with no unit is a byte count. The prefixes are decimal, so `"128G"` and `"128GB"` both mean 128 GB. A worker declaring `"128G"` therefore satisfies a task requiring `"65536M"` but not one requiring `"200000M"`.

#### 3.4.2 Launchers

`ssh_launcher()` and `start_worker()` are documented in section 6.1, and the ports and reachability rules they depend on in section 6.2.

### 3.5 Execution

``` r
run_workflow(workflow,
             worker_list     = NULL,          # required in practice: errors if empty
             chunk_size      = 1e8,           # 100 MB — the default is fine, omit it
             master_host     = NULL,
             ports           = 61000:61099,    # master's per-remote-worker listen pool
             log_dir         = NULL,
             startup_timeout = 30,
             shared_path     = NULL)          # R/execution.R
```

-   `workflow`: a `workflow_object`. A simple `task_object` is rejected, so wrap it first with `new_workflow(workflow_id = "wf", task_list = list(my_task))`. There is no auto-coercion, and the error states exactly this.
-   `worker_list`: the list of `new_worker()` objects the run may dispatch to. At least one is required: both the `NULL` default and an empty list are rejected with `"'worker_list' must contain at least one worker"`. A single worker still has to be wrapped, `worker_list = list(w)`. A worker object is itself a list, so `worker_list = w` has non-zero length and would otherwise reach the later checks; it is caught by its own error, `"'worker_list' must be a list of workers, not a single worker"`.
-   `chunk_size`: max bytes per NNG chunk. Defaults to `1e8` (100 MB).
-   `master_host`: master's IP or hostname as seen by remote workers. If `NULL`, it is auto-detected as this machine's hostname, so the workers must be able to resolve it. Set it explicitly on multi-interface or VPN setups.
-   `ports`: the contiguous TCP port range the master uses for its inbound listen sockets. `ports[1]` is the shared notify port and two further ports are taken per remote worker, so `length(ports) >= 1 + 2*n`. Default `61000:61099`. Local workers do not consume from this pool. The full model, the port plan printed at start and the overlap rule are in section 6.2.
-   `log_dir`: directory for local subprocess logs. With `NULL`, `log/` under the working directory is used, created if missing, and the resolved path is printed at start. Remote workers do not inherit it: they write to `log/` under their own `root_path` on their own machine. `run_workflow()` always blocks until the workflow finishes. To start a run without occupying the R session, launch the script as a background OS process, for example `nohup Rscript pipeline.R &`.
-   `startup_timeout`: seconds to wait for the expected workers to register with the scheduler. Default 30. It is one deadline for the whole pool, not one per worker, and it covers only the handshake. A worker that registers in time and stalls later is caught by the thresholds of section 9.3 instead. On expiry the run is torn down and reports the workers that never registered as unreachable from the scheduler, in the same matrix the connectivity check prints.
-   `shared_path`: character vector of shared-filesystem root paths. A file parameter whose path lies at or under one of these roots is read by the consumer from that root instead of being transferred peer-to-peer. The comparison is literal, so both the root and the file's declared path must be absolute: a relative `new_file("data/ref.txt")` is never matched against a root, even when it resolves under one on the master, because the same relative path resolves to a different directory on each worker (section 4.1.3). An unmatched path is transferred, with no warning. At start `run_workflow()` prints, for every file parameter, whether it was classified `SHARED` (read from the root) or `TRANSFER` (sent peer-to-peer); check that plan whenever a file you believe is shared. `NULL`, the default, means no shared filesystem, and every file is transferred.

On failure the run stops. The first instance to fail ends the whole workflow. `run_workflow()` raises an R error and the report names the task, the `exe_id`, the wildcard binding, the worker and the original error text. Instances that had already finished keep their outputs, instances still running are killed, and instances not yet started never run, which is why a failed run leaves a mixture of `finished`, `failed`, `aborted` and `ready` rows in `flumen_status()` (section 9.2). Nothing is rolled back, so an output written just before the failure stays on disk, and it can belong to an `aborted` instance just as it can to a `finished` one. Wrap the call in `tryCatch()` when the script has to carry on.

## 4. Writing tasks: conventions

### 4.1 Rules common to every task type

These hold for R, Python and shell tasks. The per-language sections that follow add only what is specific to that language.

#### 4.1.1 Escapes in a `new_script()` body

**Symptom**: a shell task fails with `bash status 127` even though every binary it invokes is present and on `PATH`.

**Cause**: in R single-quoted strings, a backslash followed by a literal newline is parsed as a newline alone, because the backslash is silently consumed. So this:

``` r
exec = new_script('
  fastp \
    -i {in_fq} \
    -o {out_fq}
')
```

does not produce a shell line continuation. After R parsing + substitution, bash reads:

```         
fastp
  -i fastq/sample.fq.gz
  -o fastq/trim/sample.fq.gz
```

Bash interprets this as three separate commands: `fastp` (no args, exits non-zero), then `-i fastq/sample.fq.gz`, read as a command called `-i` that is not in PATH and therefore exiting 127, then `-o ...` in the same way. The first 127 is reported as `Executing 'bash' failed with status 127`.

**Remedy** (in order of preference):

1.  Use `new_cmd()` if the task is a single tool invocation (see section 4.4). This removes the problem at its source, since no bash and no script are involved.

2.  Single-line commands in `new_script()` (no `\` at end of any line):

    ``` r
    exec = new_script('
      #!/bin/bash
      set -euo pipefail
      fastp -i {in_fq} -o {out_fq} -h {html} -j {json} -w {threads}
    ')
    ```

    Each shell command occupies its own logical line, and multiple commands are separated by newlines.

3.  R raw strings when multi-line continuation is required:

    ``` r
    exec = new_script(r"(
      #!/bin/bash
      fastp \
        -i {in_fq} \
        -o {out_fq}
    )")
    ```

    Raw strings disable all R escape processing, so `\<newline>` survives intact and bash interprets it as the intended line continuation.

This applies to Python bodies too, and the consequence there is more severe. R interprets the escape while parsing the single-quoted string, before the body ever reaches the interpreter, so `fh.write(x + "\n")` becomes a Python string literal containing a real newline. The result is a `SyntaxError` or `IndentationError` pointing at line 1, with nothing in the message connecting it to the escape. Write `print(x, file=fh)`, or double the backslash. The same substitution is harmless in an R body, where a newline character and `"\n"` are equivalent for `strsplit()`.

#### 4.1.2 Return values, outputs and multi-output

R / Python return shape. Both languages follow the same rules, and the per-language column shows the idiomatic return value.

| `new_output(...)` declaration | R returns | Python returns | Notes |
|------------------|------------------|------------------|------------------|
| 1 plain output `out = new_object(...)` | any single value (df, vector, scalar, list, matrix) | `DataFrame` / `Table` / `ndarray` / `list` / scalar (`int`/`float`/`str`/`bool`) | Single value auto-wrapped. The return type is detected automatically. |
| 1 plain output `out = new_file("path.ext")` | ignored, write the file in the function body | ignored, write the file in the function body | The declared path must not exist before the task runs (the wrapper refuses it, naming the file) and must exist after (the wrapper fails the task otherwise). Only the declared path is delivered: other files written into the same directory are ignored and not sent downstream. (R, Python and shell alike.) |
| Dynamic output `out = new_object("{{N}}")` | `list(v1, v2, ..., vN)` (positional) | `[v1, v2, ..., vN]` (positional), multiple outputs only if `{{N}}` is declared (see Multi-output discriminator below) | Potentially, one expandable downstream instance per element. |
| Dynamic output `out = new_file("dir/{{any}}.ext")` | ignored, write the files matching the pattern in the body | ignored, same rule | Wrapper globs `dir/{{any}}.ext` post-execution; each match becomes one output instance. |
| Named multi-output (any of the above) | named list: `list(a=v1, b=v2, ...)` | dict: `{"a": v1, "b": v2, ...}` | Names move together with the values and reach the consumer (see "Named multi-output IPC parity" below). |

Multi-output discriminator. R and Python wrappers use the output declaration (presence of `{{N}}` dynamic wildcard) as the primary signal that a list/dict return means multi-output. A return can be split only because it is a `list` or a dict, never because of its `length()`. An atomic vector returned under a dynamic object output stays one output: `f <- function(x) c("a","b","c")` with `new_output(o = new_object("{{v}}"))` arrives to the consumer as a single character vector of 3, not 3 instances. Return a `list` when you do want N outputs. Without `{{N}}`:

-   R `list(v1, v2)` returned for a static `out` yields 1 output that is a list.
-   Python `[v1, v2]` returned for a static `out` yields 1 output that is a vector, or a list when any element is a `DataFrame`, `Table` or `ndarray`.

An empty collection under `{{N}}` is refused, in both languages. R `list()` and Python `[]` / `{}` stop the run at the producer rather than send nothing downstream. R `integer(0)`, Python `np.array([])`, `""`, an empty `DataFrame` pass since an atomic return is one output whatever its length (the rule just above). In Python there is no way to write an empty atomic value as a literal: [] is a list, i.e. the collection form, so it always means zero outputs. Use `np.array([])`, `""` or an empty `DataFrame` to return one empty output.

Concrete examples (R and Python):

``` r
# Single output, scalar return (both languages accept scalars directly)
exec = new_script('f <- function(x) x + 1')                  # R: returns numeric scalar
# Python equivalent:
exec = new_script('def f(x):
    return x + 1                                              # Python: int, str, etc.
')

# Multi-instance via {{N}}: list of N
output = new_output(out = new_object("{{out}}"))
exec   = new_script('f <- function(df) list(df, df, df)')    # R: 3 potential exp instances
exec   = new_script('def f(df):
    return [df, df, df]                                 # Python: 3 potential exp instances
')

# Named multi-output (keys preserved across IPC via elem_name field)
output = new_output(out = new_object("{{out}}"))
exec   = new_script('f <- function(x) list(a = x*2, b = x*3)')   # R: named list
exec   = new_script('def f(x):
    return {"a": x*2, "b": x*3}                                  # Python: dict
')

# Multiple distinct static-named outputs. Each pattern needs static text of its
# own ("res_", "sum_"): two "{{a}}" / "{{b}}" with no static text are rejected at build time,
# because a consumer would have no way to say which one it wants (see 10.1).
output = new_output(a = new_object("res_{{a}}"), b = new_object("sum_{{b}}"))
exec   = new_script('f <- function(x) list(a = x*2, b = x*3)')   # R: named list
exec   = new_script('def f(x):
    return {"a": x*2, "b": x*3}                                  # Python: dict
')

# Side-effect file output (both languages support side-effect file writes)
output = new_output(out = new_file("out/{{any}}.txt"))
exec   = new_script('f <- function(x) {                       # R: returns NULL OK
    writeLines(as.character(x), "out/a.txt")
    writeLines(as.character(x*2), "out/b.txt")
  }')
exec   = new_script('def f(x):                                # Python: returns None OK
    open("out/a.txt", "w").write(str(x))
    open("out/b.txt", "w").write(str(x*2))
')
```

#### 4.1.3 Outputs are never parameters, and where relative paths resolve

-   In an R or Python task, `new_output(...)` parameters are stripped from the call and never reach the user function, whose signature carries the `new_input` names only, so a body that needs its own output path must rebuild it from an input carrying the same wildcard value, as below. A shell task is the exception, since a command must be given its output path: there a `{name}` placeholder is substituted from `new_output` as well, and under `new_cmd()` the resolved output path becomes a command-line argument (section 4.4).

-   A task that declares a `new_file()` output has its return value ignored. The wrapper enumerates the files produced instead. An object output and a file output cannot share one task: split them in two. Declaring both is rejected at build time.

-   A local worker runs in the master's working directory. Relative paths resolve there, both in a task body and in `new_file()` declarations, so outputs are placed in the directory R was launched from, which is not necessarily the one holding the script. This resolution is for reading and writing the file; it does not extend to `shared_path` matching, which compares absolute paths only (section 3.5). A worker with a launcher behaves differently. See section 6.1 on `root_path`.

Writing to an output path that contains a static `{wc}`. The resolved output path never reaches the function, so the body has to rebuild it from the input that carries the same wildcard value. Declaration and reconstruction must agree character for character, or the run fails with `declared output '<p>' but the task did not produce it`. The same path must be written twice, once in the declaration and once in the body:

``` r
new_R_task(
  task_id = "greet",
  input   = new_input(s = new_object_exp("{sample}")),   # s == the {sample} value
  output  = new_output(o = new_file("out/{sample}.txt")),
  exec    = new_script('
    f <- function(s) writeLines(paste("hello", s),
                                file.path("out", paste0(s, ".txt")))
  ')
)
```

Python behaves identically, with `os.path.join("out", s + ".txt")` against `new_file("out/{sample}.csv")`. This is why an output whose static wildcard is not also bound by an input has no way to be written: keep the driving wildcard on an input parameter.

When the wildcard's values are objects, ask for the pattern, not the value. The sentence above -- rebuild the path from the input carrying the same wildcard value -- reads differently once the axis is object-valued, and the immediate reading does not work: `new_object_exp("{data}")` hands the function the data frame, and there is nothing in it from which to recover `"df1"`. The key is reachable, by the rule in section 3.1: a token standing alone is replaced by the object, a token with any text around it is replaced by the key. So ask for the finished path instead of assembling it, and the two strings that must agree are then written identically, side by side:

``` r
new_R_task(
  task_id = "save",
  input   = new_input(
    fname = new_object_exp("{data}_result.Rds"),        # arrives as "df1_result.Rds"
    d     = new_dep("upstream", new_object("{{out}}"))
  ),
  output  = new_output(o = new_file("{data}_result.Rds")),
  exec    = new_script('f <- function(fname, d) saveRDS(d, fname)')
)
```

With atomic values both spellings give a string anyway, since `"{sample}"` arrives as `"s1"` and `"{sample}.txt"` as `"s1.txt"`, so assembling the name in the body or asking for it already finished are equally correct. The two spellings part company only when the values are objects, where the token alone hands over the object while a token with text around it yields the key.

#### 4.1.4 The tool must be on the worker's `PATH`, and at the same path on every node

**Symptom**: a shell task fails with `Shell command failed with exit status 127`, and stderr names the tool:

```         
/tmp/RtmpZnrTc4/flumen_op_7e376e9d0cc8.sh: line 4: macs2: command not found
```

This is not the 127 of section 4.1.1. What the two have in common is only the exit status, and the name bash could not find distinguishes them. There the script was malformed and bash read an option as a command, so the name is a flag such as `-i` or `-o`, a name never intended to be executed. Here the script is well formed and the name is the intended tool, absent from the `PATH` of the process that runs it.

**Cause**: the wrapper writes the body to a temporary script and runs it with `shell_cmd`. The script therefore inherits the environment of the worker process, which inherits it from whatever started that worker. A remote worker is started over SSH, and a non interactive SSH session does not read a login profile, so a `conda activate`, a module system, or anything appended to `PATH` in `.bash_profile` is not there. Section 9.3 states the same fact for Flumen's own threshold variables: SSH does not forward an environment and the remote bootstrap exports no variable of its own.

This affects only remote workers, which is why a workflow that runs on a laptop can fail on a cluster with no change to the code. There is no worker level environment setting. `new_worker()` accepts `R_cmd`, `python_cmd` and `shell_cmd`, and nothing that sets `PATH` or any other variable.

**Remedy** (in order of preference):

1.  Export the directory as the first line of the script. This covers the tool and anything the tool calls in turn, which matters for a Python entry point such as `macs2` or `bamCoverage`:

    ``` r
    exec = new_script('
      #!/bin/bash
      set -euo pipefail
      export PATH="/opt/tools/bin:$PATH"
      macs2 callpeak -q {q} -t {ip} -c {input} --outdir {outdir}
    ')
    ```

2.  Pass the binary as an input parameter, which makes the dependency visible in `preview_workflow()` (section 9.2).

    ``` r
    callpeak <- new_shell_task(...,
      input = new_input(macs2_bin = "/opt/tools/bin/macs2", ...),
      exec  = new_script('
        #!/bin/bash
        set -euo pipefail
        {macs2_bin} callpeak -q {q} -t {ip} -c {input} --outdir {outdir}
      ')
    )
    ```

**Constraint**: with more than one node the path must be identical on all of them, because the script text is built once and runs unchanged on whichever worker the scheduler picks. A tool at `/opt/tools/bin/macs2` on one node and at `~/.local/bin/macs2` on another then succeeds or fails depending on placement, and placement can differ between runs. The same holds for a tool name given without an absolute path that is on the `PATH` of some nodes only.

Satisfy it either by making the installation uniform across every node that can run the task, or, when that is not possible, by pinning the task to the nodes that have the tool, with `requires` matching the worker's `resources` (section 3.4.1):

``` r
callpeak <- new_shell_task(..., requires = list(tags = "has_macs2"))
new_worker(name = "node07", resources = list(tags = "has_macs2"), ...)
```

Pinning turns a 127 that appears on some runs and not others into a scheduling constraint, which either is satisfied or is reported before any task executes.

Fixing one tool does not fix the next one. Installation mechanisms do not share a directory: `pip install --user` puts entry points in `~/.local/bin`, conda puts them in that environment's `bin`, and a module system prepends its own. A pipeline that calls several external tools can need several of these directories, and each missing one is reported only when its task is finally reached, which on a long pipeline can be hours in.

### 4.2 R task

``` r
new_R_task(
  task_id = "T",
  input   = new_input(x = 1, y = "abc"),
  output  = new_output(out = new_object("{{out}}")),
  exec    = new_script('
    my_func <- function(x, y) {
      list(result = paste(x, y))
    }
  ')
)
```

**Rules**:

-   The script body is parsed and evaluated in an isolated environment, and the value of the last top-level expression must be a function (the callable that Flumen invokes). In practice this means the script ends with a function definition like `f <- function(...) { ... }`. R has no scalar type, so the rule is simpler than the Python one: an aggregation arrives as a `list`, everything else arrives as the value itself.

| Declared as | The function receives |
|------------------------------------|------------------------------------|
| `new_object_exp("{wc}")` | the wildcard value |
| `new_dep(..., new_object("{{v}}"))` | the produced value, unwrapped |
| `new_dep(..., new_file_exp("f_{wc}"))` | the path, a `character` of length 1 |
| `new_dep(..., new_object_aggr("{{v}}"))` | `list` of N |
| `new_dep(..., new_file_aggr("f_{wc}"))` | `list` of N paths |
| `new_dep(..., new_file("f_{{dyn}}"))` on a producer that emitted N files | `list` of N paths |

The first row is not a dependency. It is the value the instance was cloned for, which is how a body knows which sample it is running on. The second takes one producer instance's object and hands it over as the value. The third gives each consumer instance the one file its own `{wc}` selected, which is the ordinary per-sample dep and the one worked out below. The fourth and fifth gather across producer instances, and an `_aggr` always arrives as a list even when it gathers a single element, so code written for many samples needs no special case for one. The sixth is a plain dep that still arrives as a list, because the producer's dynamic output resolved to several files (section 7.5). A list therefore has two independent causes, declaring an `_aggr` or having more than one file.

The consumer runs once per `{sample}` and receives the single path produced by its sample:

``` r
align <- new_R_task(
  task_id = "align",
  input   = new_input(s = new_object_exp("{sample}")),
  output  = new_output(o = new_file("bam/{sample}.bam")),
  exec    = new_script('f <- function(s) writeLines(s, paste0("bam/", s, ".bam"))')
)

count <- new_R_task(
  task_id = "count",
  input   = new_input(s   = new_object_exp("{sample}"),
                      bam = new_dep("align", new_file_exp("bam/{sample}.bam"))),
  output  = new_output(o = new_file("counts/{sample}.txt")),
  ## bam is a length-1 character: the path of this instance's bam.
  exec    = new_script('
    f <- function(s, bam) writeLines(readLines(bam)[1], paste0("counts/", s, ".txt"))
  ')
)
```

Use `new_file_exp` and not a plain `new_file` inside the dep: alongside the `new_object_exp("{sample}")` driver, a non-`_exp` value on the same wildcard axis is a mixed expansion driver and is rejected at build time (section 5.2). The `_aggr` form is the documented alternative, exempt from that check and filtering to the same single file, but it arrives as a one-element `list` rather than as a simple path.

The defensive idiom `paths <- if (is.list(fs)) unlist(fs) else fs` is therefore only needed when one body must handle both an aggregated and a non-aggregated parameter.

The following table shows the output received by a producer when it defines a single `new_object()` dep:

| Producer Returns | Consumer receives |
|------------------------------------|------------------------------------|
| `42` | `numeric`, length 1 |
| `list(42)` | `numeric`, length 1, since the one-element list is unwrapped |
| `list(result = 42)` | `numeric`, length 1, unwrapped, and the name is dropped |
| `list(1, 2)` | `list` of 2, no names |
| `list(a = 1, b = 2)` | `list` of 2, names `a`,`b` preserved |

So a multi-element list does arrive whole on a plain dep. Under `new_object_aggr()` the elements are spliced into the aggregate rather than kept per instance (section 7.2.1).

The two contexts also differ for a one-element named list, which is why section 7.2.1 recommends `setNames(list(x), id)` while the table above says the name is dropped. On a plain dep the wrapper is unwrapped and the name lost, identically in R and in Python (section 4.3.1), whereas under aggregation the names arrive with the values. The recipe in section 7.2.1 is for the aggregated case only.

-   The function arguments must match the names declared in `new_input(...)` only, and a mismatch on input names raises a runtime error.

-   Helper functions defined before the main one are accepted, and only the last assignment or expression is treated as the entry point.

-   For `new_object()` output, the function's return value is captured as the output. The shape required depends on the output declaration, as the table below shows. For a `new_file()` output the return value is ignored, so return `NULL` / `None`.

### 4.3 Python task

``` r
new_python_task(
  task_id = "T",
  input   = new_input(df = ...),
  output  = new_output(out = new_object("{{out}}")),
  exec    = new_script('
    def my_func(df):
        return {"out": df.shape[0]}
  ')
)
```

**Rules**:

-   Same name-matching rules for arguments and output keys.
-   The entry point is the last top-level `def` of the body, whatever its name, so helper functions must be defined before it.
-   Accepted return types: `pandas.DataFrame`, `pyarrow.Table`, `numpy.ndarray`, Python `list`, Python `dict` (named multi-output), and scalars (`int`/`float`/`str`/`bool` plus their numpy counterparts). Any other type raises a `TypeError` at the wrapper. A 1-D `numpy.ndarray` moves as a vector so an R consumer receives an atomic vector (`num [1:3]`, a scalar at length 1), a Python consumer an `ndarray` of shape `(n,)`. A 2-D array moves as a matrix, so an `(n, 1)` array stays an `n x 1` matrix; return `arr.ravel()` to send it as a vector. A `dict` triggers named multi-output, in parity with an R named-list return. There is no `qs2` fallback in Python, so anything that is not Arrow-compatible (closures, S4-like objects, complex Python types) must be marshalled through a file instead.
-   File output via side-effect is supported also in Python. The declaration `new_file("dir/{{any}}.ext")` writes the files in the function body. The wrapper enumerates new/modified files post-execution and ignores any explicit return value.
-   A value of length 1 arrives as a Python scalar, longer ones as a `list`. The shape therefore depends on the data, not on the declaration. It does not depend on whether the producer named its result. `list(a = 10)`, or the Python `dict` equivalent, arrives on a plain dep as `10` with the name gone, and under `new_object_aggr()` as a list of one still named `a`. The following table shows the Python type of each argument by the form the input was declared in:

| Input declared as | Python function receives |
|------------------------------------|------------------------------------|
| dep on an object of length 1 | a scalar: `int`, `float` or `str` |
| dep on an object of length 3 | `list` of 3 |
| a `_exp` wildcard value | `str`, a scalar and not a length-1 list |
| `new_*_aggr` over N producers | `list` of N, each element collapsed by the same rule |

This is the same convention of R consumers (a one-element list is unwrapped to the value itself). Aggregating N files would yield `list[list[str]]` instead of `list[str]`.

A single element is the dangerous case. When the producer's dynamic output returns three values the parameter is a `list` of three, and `for x in v` iterates the three. When it returns one, the parameter is that one value, and the same loop iterates the characters of the string instead, with no error. An aggregation is not exposed, since `new_object_aggr()` delivers a list of one. Normalise the parameter in the first line of the body, in whichever direction the code needs:

``` python
def f(sample, files):
    s     = sample if isinstance(sample, str) else sample[0]   # scalar wanted
    paths = files if isinstance(files, (list, tuple)) else [files]  # list wanted
```

-   Aggregated inputs (`new_*_aggr`) arrive as a Python `list` when positional, or as a `dict` when the producer used named multi-output (see "Named multi-output IPC parity" below).

-   `matrix` / `data.frame` inputs and row/column labels. An R `matrix` arrives as a `numpy.ndarray` of the numeric values only: `ndarray` has no label axis, so row/column names are dropped. An R `data.frame` arrives as a `pandas.DataFrame` with its column names preserved and its character `rownames` restored as the DataFrame index, symmetrically with the R to R case. To keep a matrix's labels, either (a) pass a `data.frame`, since a single object carries both axes, with rownames becoming the index and colnames the columns, or (b) pass the matrix together with a separate names vector, which arrives as `list[str]`, and rebuild the labelled object in Python, for example `pandas.DataFrame(mat, index=genes, columns=cells)` or an `AnnData` with `X`, `var` and `obs`.

-   Orientation is preserved in both directions, and only the labels are dropped. An R `matrix(1:15, nrow = 3)` (3 genes × 5 cells) arrives as a `numpy.ndarray` of shape `(3, 5)` with `a[0, 1] == m[1, 2]`. A numpy `arange(15).reshape(3, 5)` arrives as an R 3×5 matrix whose first column is numpy's first column.

-   What can cross, and what happens when it cannot. The transport is Arrow, and Flumen never re-serialises a payload. The producer encodes Arrow when the type allows and `qs2` otherwise, without knowing who will consume it. Arrow carries atomic vectors, matrices and data.frames. Every other type, including a sparse `Matrix::dgCMatrix`, an S4 object, a nested list and a `complex` vector, is refused before the Python wrapper receives it, in a single line naming the parameter, the consuming task and the class. Convert before returning: `as.matrix()` or the triplets for a sparse matrix, flattening for a nested list, `Re()` and `Im()` for a complex vector. The alternative is to write the object to a file and pass the file, which is the only route for a payload Arrow cannot express. A sparse count matrix therefore does not cross as an object: pass it as a file, or as the dense matrix if it fits. The Python to R direction refuses the same way and always has (`TypeError: Unsupported type: ...`).

-   A missing value can change the type of a `data.frame` column on a round trip through Python. Every case in the table below is silent, and since the values and the totals come back unchanged, nothing you would normally check will reveal it.

    | R sends | Python's function sees | R receives back |
    |------------------------|------------------------|------------------------|
    | `integer`, even one `NA` | `float64` | `double` |
    | `integer`, all `NA` | `float64` | `double` |
    | `logical`, all `NA` | `object` | `vctrs_unspecified` |
    | `character`, all `NA` | `object` | `vctrs_unspecified` |
    | `double` holding a real `NaN` | `nan` | `NA` (the `NaN`/`NA` distinction is gone) |

    Everything else crosses intact, including `logical` with an `NA` among values, `integer` with no `NA`, the 32-bit integer limits, `Inf`/`-Inf`/`-0`/17 significant digits, `character` with `NA`, accents and empty strings, `factor` (with and without `NA`), `Date` and `POSIXct`. A Python `DataFrame` with zero rows loses the type of its `object` columns the same way, since there is no value left to infer one from. The cause is the pandas conversion, not the transport. NumPy-backed pandas dtypes cannot express a missing value inside an integer, logical or text column, so `to_pandas()` promotes the column, and once promoted nothing records for R what it was.

    **Remedy**: state the type in the Python body with a nullable dtype, `df["x"] = df["x"].astype("Int64")` (or `"boolean"`, `"string"`, `"Float64"`). This carries the type through, and `Float64` also keeps a real `NaN` distinct from a missing value.

-   A timestamp with no explicit time zone shifts the clock. R and pandas interpret a value with no zone differently. R stores an instant and prints it in the machine's zone, while pandas and Arrow store a wall clock and read it as UTC. The integer crosses unchanged, so the instant is always correct. What moves is the clock the other side reads, by that machine's UTC offset. Daylight saving is included in that offset, so the shift is two hours in June where it is one in January. On a machine set to `Europe/Rome`:

    | Written in | Value | The other side reads |
    |------------------------|------------------------|------------------------|
    | R | `as.POSIXct("2020-06-15 23:30:00")` | `2020-06-15 21:30:00` in Python |
    | Python | `pd.Timestamp("2020-06-15 23:30:00")` | `2020-06-16 01:30:00` in R |

    **Remedy**: specify the zone. `as.POSIXct(x, tz = "UTC")` in R, `s.dt.tz_localize("UTC")` in Python. A column with an explicit zone crosses with the same clock and the same instant in both directions, including a zone that is not the machine's own (`America/New_York` is carried through unchanged). One detail to expect: a zone stated explicitly but equal to the machine's own comes back with the attribute dropped, which changes what `attr(x, "tzone")` reports but not the printed time.

-   Worker runtime dependencies: `pyarrow`, `pynng`, `pandas` and `numpy`. The wrapper imports all four unconditionally, so a Python task does not start without them, whatever it passes. Configure the interpreter with `new_worker(python_cmd = "/path/to/python3")`.

-   Automatic dedent, so the body may be indented freely inside the R string.

#### 4.3.1 Named multi-output IPC parity (R↔Python)

When a producer returns a named multi-output (R `list(a=v1, b=v2)` or Python `{"a": v1, "b": v2}`), the names move together with the values and reach the consumer with a language-idiomatic shape, symmetric but not identical, because a Python `dict` cannot hold duplicate keys while an R named list can.

Consumer-side shape table on aggregation:

| Producer return | R consumer receives | Python consumer receives |
|------------------------|------------------------|------------------------|
| `list(s=v)`, one named element on a plain dep (no `_aggr`) | `xs = v` (name dropped) | `xs = v` (name dropped) |
| `list(a=v1, b=v2)` from 1 producer instance | `xs = list(a=v1, b=v2)` (named list) | `xs = {"a": v1, "b": v2}` (dict) |
| `list(a=v1, b=v2)` from N producer instances (composite) | `xs = list(a=v1, b=v2, a=v3, b=v4, ...)` (named list with duplicate keys) | `xs = {"a": [v1, v3, ...], "b": [v2, v4, ...]}` (dict-by-name, values grouped) |
| Positional `list(v1, v2)` / `[v1, v2]` (no names) | `xs = list(v1, v2, ...)` (unnamed) | `xs = [v1, v2, ...]` (list) |

R's idiom keeps the duplicate-key list, and the consumer indexes either positionally (`xs[[1]]`) or by name (`xs[names(xs) == "a"]`). Python's idiom groups by name into a `dict` of `list`, since `dict` keys are unique by construction.

If the consumer takes one value per sample it already knows which sample it is, since the instance is bound to that wildcard value and a second input carrying the same wildcard gives the body the label (section 3.1). No names are needed. Identity becomes a problem only in an aggregation, where one consumer instance receives the values of N producer instances. They arrive in whatever order those instances finished, so position cannot identify them and `xs[[1]]` can belong to a different sample on the next run. When the pipeline needs a gather and the consumer has to distinguish the values, have the producer name its return with the sample, `setNames(list(x), id)` in R or `{id: x}` in Python, and index by name, `xs[["S1"]]`. A file gather needs none of this, since the sample is in the path and `basename()` recovers it whatever the order.

### 4.4 Shell task

Prefer `new_cmd()` over `new_script()` when invoking a single tool with no shell logic. With `new_cmd("toolname")` Flumen builds the argv list directly from `new_input` / `new_output` and executes the binary with no bash interpreter involved, which removes the quoting, globbing and escaping pitfalls (see section 4.1.1). Use `new_script(...)` only when shell features are required: derived variables, multiple commands, redirections, conditionals or piping.

``` r
# Recommended for single-tool tasks: new_cmd()
new_shell_task(
  task_id = "trim",
  input   = new_input(
    "-w" = 4,
    "-i" = new_file_exp("fastq/{sample}.fastq.gz")
  ),
  output  = new_output(
    "-o" = new_file("trim/{sample}.fastq.gz"),
    "-h" = new_file("reports/{sample}.html"),
    "-j" = new_file("reports/{sample}.json")
  ),
  exec    = new_cmd("fastp")
)

# Use new_script() only when shell logic is required
new_shell_task(
  task_id = "align",
  input   = new_input(
    "threads" = 8,
    "genome"  = new_file("../genome/{ref}.fa"),
    "reads"   = new_file_exp("../fastq/{sample}.fastq")
  ),
  output  = new_output(
    "sam" = new_file("sam/{sample}.sam")
  ),
  exec    = new_script('
    bowtie2 -p {threads} -x {genome} -U {reads} -S {sam}
  ')
)
```

**Rules**:

-   Placeholders `{name}` in the script body are substituted with the matching parameter value from `new_input` / `new_output`.

-   Files are passed as path strings (plain substitution).

-   For `new_cmd("fastqc")`, inputs/outputs are passed positionally as CLI arguments in the order they appear in `new_input` / `new_output`. Names like `"--threads" = 10` or `"-o" = ...` become CLI flags. Unnamed entries such as `new_input(..., "--star", ...)` pass through positionally, which is what flags taking no value require. Several unnamed entries in one task are normal and allowed: they are that tool's positional arguments (`mv src dst`).

-   Exception: an object output never reaches argv. A shell task whose output is `new_object(...)` declares that its result is its standard output, which Flumen captures. There is no path to hand to the tool, and the value is still the unresolved `{{name}}` pattern. That slot is therefore skipped when the argv is built. A file output is passed normally, which is what you want for `fastp -o trimmed.fq`. Flumen creates the parent directory of every `new_output(...)` automatically for all task types, R, Python and shell.

-   Do not rely on `\<newline>` line continuation inside `new_script(...)` with R single-quoted strings. See section 4.1.1 for the pitfall and the safe patterns.

#### 4.4.1 Capturing a shell task's standard output

Declare an object output and no file output: Flumen captures whatever the command writes to stdout and delivers it downstream. The consumer receives it as raw bytes, so an R task converts it with `rawToChar()` (a Python task with `.decode()`).

``` r
A <- new_shell_task(
  task_id = "A",
  output  = new_output(out = new_object("{{x}}")),
  exec    = new_script('echo "hello world"')
)

B <- new_R_task(
  task_id = "B",
  input   = new_input(txt = new_dep(task_id = "A", value = new_object("{{x}}"))),
  output  = new_output(out = new_object("{{y}}")),
  exec    = new_script('
    f <- function(txt) trimws(rawToChar(txt))     # -> "hello world"
  ')
)
```

To write to a file instead of capturing stdout, declare the file as the output and redirect to it. The redirection is shell logic, so this requires `new_script()` rather than `new_cmd()`:

``` r
new_shell_task(
  task_id = "greet",
  input   = new_input(sample = new_object_exp("{sample}")),
  output  = new_output(out = new_file("{sample}.txt")),
  exec    = new_script('echo "ciao {sample}" > {out}')
)
```

Keeping `new_cmd()` for a tool that writes to stdout. The redirection above forces `new_script()`, which collides with the best practice at the top of this section for single-tool invocations (`wc`, `grep`, `samtools view`, …). A third route keeps `new_cmd()`: declare the object output, whose slot is skipped when argv is built so that stdout is captured, and let a downstream task materialise it, as with `wc -l`:

``` r
count <- new_shell_task(                      # wc -l data/sorted.txt
  task_id = "count",
  input   = new_input("-l", new_dep("sort_lines", new_file("data/sorted.txt"))),
  output  = new_output(count = new_object("{{count}}")),
  exec    = new_cmd("wc")
)
write_count <- new_R_task(                    # stdout arrives as raw bytes
  task_id = "write_count",
  input   = new_input(wc_out = new_dep("count", new_object("{{count}}"))),
  output  = new_output(o = new_file("data/line_count.txt")),
  exec    = new_script('
    f <- function(wc_out) {
      n <- strsplit(trimws(rawToChar(wc_out)), "[[:space:]]+")[[1]][1]
      writeLines(n, "data/line_count.txt")
    }
  ')
)
```

Note the two unnamed inputs on the `new_cmd` task: `"-l"` is a simple flag and the `new_dep` is a positional operand, whereas a name would have become a CLI flag. Several unnamed parameters in one task are allowed and do not trip the duplicate-name check.

#### 4.4.2 Backticks in paths with `{{N}}`

Some tools require a base name or an output directory and decide the file names themselves. `bowtie2-build` takes `index/idx` and writes `idx.1.bt2`, `idx.2.bt2` and so on. Backticks in the consumer's dep mark the part of the pattern the command receives, so the shell task receives that text as one string instead of the list of paths.

``` r
new_file("`index/idx_base`.{{N}}.bt2")   # basename prefix + numeric index + extension
new_file("`outdir/`{{any}}.txt")         # directory prefix + runtime-decided filenames
```

Those are the only two shapes: the backticks always delimit static text, either a basename prefix or a directory prefix. The prefix is substituted verbatim, meaning exactly the text between the backticks, including a trailing `/` where one was written. Do not add a separator that the pattern already carries:

``` r
# producer: tracks every file under rsem_index/ as a single dynamic output
output = new_output("ref" = new_file("`rsem_index/`{{N}}"))

# shell consumer: {ref} -> "rsem_index/"  (the slash is preserved)
exec = new_script('
  rsem-calculate-expression --star -p {threads} {trimmed} {ref}genome {OUT_PREFIX}
')
# writing "{ref}/genome" here would yield "rsem_index//genome".
```

Dual consumer semantics:

-   Shell-task consumer: the `{name}` placeholder in the script body resolves to the static prefix (the content between backticks), used as a prefix argument by tools like `bowtie2 -x`, `rsem-calculate-expression`, `hisat2 -x`. Write the backticks on the consumer's dep, which is what requests the prefix; the producer's own output may have been declared without them. The files are still delivered either way. The backticks change what is substituted for `{name}`, not what is moved unlike a plain `character` input, which is never transferred (section 4.4.4).
-   R / Python task consumer: backticks are shell-only and are rejected at build time in any `new_R_task` / `new_python_task` parameter.

#### 4.4.3 Placeholder substitution in shell bodies

A shell `new_script()` body is scanned for `{param}` tokens only: a brace-wrapped declared-parameter name (from `new_input` / `new_output`) that is not preceded by `$`. Everything else is passed to bash untouched, so a shell task can use full bash with no escaping, including `${var}` expansions, brace groups, brace expansion and `case` or `if` constructs.

| In the script body | Substituted? |
|------------------------------------|------------------------------------|
| `{threads}`, `{out}`, a declared `new_input` / `new_output` name | yes, replaced by its value |
| `${var}`, `${v%%pat}`, `${#v}`, `${arr[@]}`, bash parameter expansion | no, left for bash. The `$` guard means bash `${name}` always wins, even when `name` is also a declared param |
| `{ cmd; }` (brace group), `{a,b}`, `{1..10}` (brace expansion) | no, a space or a non-identifier character prevents any match |
| `{{` … `}}`, including a nested block closing two levels at once such as awk's `…{…}}` | no, left exactly as written. Only the `{name}` nested inside is shielded from substitution, so `{{threads}}` stays `{{threads}}` and is not replaced |
| `{typo}`, not a declared param | left as written, with no error, so check the spelling against the `new_input` / `new_output` names |

What the single `{name}` placeholder becomes is decided by the backticks, not by the pattern and not by the choice between `_aggr` and a plain constructor. Without backticks the placeholder is always the list of resolved paths, space-separated, whether the pattern names a static `{wc}` or a dynamic `{{name}}`, so `for f in {bams}; do …` iterates them. With backticks it is the static prefix they delimit:

``` r
new_dep("p", new_file("`rsem_index/`{{N}}"))   # {r} -> rsem_index/
new_dep("p", new_file_aggr("`rsem_index/`{{N}}"))# {t} -> rsem_index/
new_dep("p", new_file_aggr("rsem_index/{{N}}"))# {u} -> rsem_index/f1 rsem_index/f2 …
new_dep("p", new_file_aggr("bam/{sample}.bam"))# {b} -> bam/s1.bam bam/s2.bam
```

The second and third lines name the same producer output and differ only in the backticks. Without them `{u}` becomes the resolved paths separated by spaces, so `{u}genome` reaches the command as `rsem_index/f1 rsem_index/f2 rsem_index/f3genome`, with the literal text attached to the last path alone. With them `{t}genome` reaches it as `rsem_index/genome`, which is the prefix the tool expects. Write the backticks whenever the command expects a prefix rather than a list of files.

Shell ops accept only files, atomic scalars, or atomic vectors (section 8).

#### 4.4.4 Shell input type constraints

Shell tasks consume parameters as CLI arguments. The wrapper validates upstream values before constructing argv and rejects complex types that cannot be marshalled as command-line strings.

| Upstream value type | Accepted? | Notes |
|------------------------|------------------------|------------------------|
| `character` scalar / vector | Yes | Inserted verbatim as argv tokens. |
| `numeric` / `integer` scalar / vector | Yes | Coerced to character. |
| `logical` scalar / vector | Yes | Coerced to `"TRUE"` / `"FALSE"`. |
| File path (`new_file()`) | Yes | Path string passed to the command. |
| `list` whose elements are all atomic | Yes | For example the paths of a `new_file_aggr` over a static `{wc}` or a dynamic `{{name}}`, which reach the body space-separated. Only a dep written with backticks does not arrive as a list, since it substitutes to the static prefix (section 4.4.2). |
| `data.frame` | No | Error: "cannot consume parameter '<name>' as data.frame". |
| `matrix` / `array` | No | Same family of error. |
| `raw` (bytes) | No | Same. |
| `list` with non-atomic elements | No | Same. |

**Remedy**: have the upstream R/Python task write the complex value to a `new_file("path.tsv")` / `.rds` / `.parquet` and let the shell task consume the file path instead. This keeps the IPC contract unambiguous.

A reference read by prefix or directory is a plain string, not a `new_file()`. A tool that reads a reference by a path prefix or directory (`bowtie2 -x`, `bwa`, `STAR --genomeDir`, `hisat2 -x`, RSEM prefix) should receive that path as a plain `character` input, because the prefix (`.../human_index`) is not a single file, the files being `human_index.1.bt2`, `.2.bt2` and so on. A string is substituted into the command and is neither transferred nor existence-checked, and the tool resolves the set itself. Use `new_file()` or `pre_staged()` only when the reference must move peer-to-peer to another worker, in which case reference the actual files (`new_file(c(".1.bt2", ".2.bt2", …))`) while still passing the prefix to `-x`. This is distinct from the backtick output-prefix mechanism (section 4.4.2), which tracks a producer's dynamically-named files.

## 5. Expansion, aggregation and outputs

This section describes how a wildcard turns one task into many instances, how a gather turns many instances back into one, and how a task declares the files it will write. These rules decide whether a workflow produces the correct values.

### 5.1 Wrapping: rejected and correct forms

The rule itself is in section 1.1. What follows is the catalogue of forms it rejects and the constructor to use instead.

``` r
# Rejected (not wrapped)
input  = new_input(x = "{sample}")                # static
input  = new_input(x = "{{val}}")                 # dynamic
output = new_output(out = "{{out}}")              # dynamic (output)
input  = new_input(x = new_dep(task_id = "A", value = "{{val}}"))  # not wrapped, inside dep

# OK — explicit intent
input  = new_input(x = new_object("{sample}"))      # vector binding (1 instance, vector value)
input  = new_input(x = new_object_exp("{sample}"))  # fan-out (N instances, scalar value)
input  = new_input(x = new_file("data_{sample}.txt"))      # vector binding, file path
input  = new_input(x = new_file_exp("data_{sample}.txt"))  # fan-out, file path
output = new_output(out = new_object("{{out}}"))                  # in-memory output
output = new_output(out = new_file("path/{{any}}.txt"))           # file with dynamic name
input  = new_input(x = new_dep(task_id = "A", value = new_object("{{val}}")))
input  = new_input(x = new_dep(task_id = "A", value = new_object_aggr("{{out}}")))
```

**Rule (mapping)**:

| User intent | Constructor |
|------------------------------------|------------------------------------|
| Fan-out task: N instances, scalar per instance | `new_object_exp` / `new_file_exp` |
| Vector binding: 1 instance, all values as vector | `new_object` / `new_file` |
| Consumer dep aggregation | `new_object_aggr` / `new_file_aggr` |
| Output slot, runtime-resolved object | `new_object("{{name}}")` |
| Output slot, file with dynamic name | `new_file("path/{{name}}.ext")` |

Wrapping is necessary, not sufficient. A correctly wrapped parameter can still be rejected by one of the other build checks, which are applied independently of this one:

-   More than one dynamic wildcard in a single parameter.
-   An `_exp` constructor used in an output.
-   Mixed expansion drivers on the same wildcard axis (section 5.2).

### 5.2 Mixed expansion / non-expansion drivers on the same wildcard axis are rejected

`new_workflow()` rejects at build time tasks whose input parameters reference the same static wildcard `{x}` with conflicting expansion intents, that is when at least one parameter uses an `_exp` constructor (`new_object_exp` / `new_file_exp`) and at least one other uses a non-`_exp` constructor (`new_object` / `new_file` / `"{x}"` / a `new_dep()` whose value is one of those). The two intents are incompatible: expansion drivers fan the task out into N instances, each receiving a scalar value, while non-expansion drivers keep a single instance with the wildcard bound as a vector (see section 5.3.2). Use the same variant for all input parameters referencing the same wildcard.

``` r
# Rejected: mixed drivers on '{sample}'
op <- new_R_task(
  task_id = "preprocess",
  input   = new_input(
    sample_id = new_object("{sample}"),                  # non-expansion
    mat_files = new_file_exp("count_{sample}/data.txt")  # expansion
  ),
  ...
)
# Flumen workflow rejected.
#   call      : new_workflow()
#   task      : preprocess
#   error     : input parameters on wildcard '{sample}' have conflicting expansion
#               drivers: 'mat_files' use new_object_exp / new_file_exp,
#               'sample_id' do not
#   fix       : use the _exp forms on every parameter to fan the task out, or none
#               of them to bind the wildcard as a vector

# OK — uniform expansion (N instances)
op <- new_R_task(
  input = new_input(
    sample_id = new_object_exp("{sample}"),
    mat_files = new_file_exp("count_{sample}/data.txt")
  ),
  ...
)

# OK — uniform non-expansion (1 instance, vector binding per section 5.1)
op <- new_R_task(
  input = new_input(
    sample_id = new_object("{sample}"),                 # wrapped, non-expansion
    mat_files = new_file("count_{sample}/data.txt")     # wrapped, non-expansion
  ),
  ...
)
```

Output parameters are not subject to this check: an output legitimately references a static wildcard expanded by inputs (e.g. `new_file("results/{sample}/out.txt")` while an input has `new_object_exp("{sample}")`). Aggregation drivers (`new_*_aggr`, with or without `new_dep` wrapping) are likewise excluded, since aggregation is a different mechanism and not a fan-out driver.

### 5.3 `_aggr` with `by` (partial aggregation)

`by` lists the wildcards to group by (SQL `GROUP BY` semantics). The consumer is cloned once per combination of `by` values; every other wildcard is aggregated into the list each instance receives. Useful for "for each `{sample}`, aggregate all `{ref}`" patterns:

``` r
new_file_aggr("counts/{sample}_{ref}.tsv", by = "sample")
# -> one consumer instance per {sample} value;
#    each receives the list of files aggregated over {ref}.
```

`by` takes one or several wildcards. With a character vector the consumer is cloned once per combination of them, and every wildcard left out is the one aggregated away, which is the usual design that collapses the replicates while keeping donor and tissue apart:

``` r
new_file_aggr("m/{donor}_{tissue}_{rep}.txt", by = c("donor", "tissue"))
# -> 2 donors x 2 tissues = 4 consumer instances;
#    each receives the list of its own 2 replicates.
```

Omitting `by` means aggregating over every wildcard that is not already fixed, not over every wildcard. When the consumer carries a `new_*_exp("{wc}")` on another parameter, and `{wc}` is one of the wildcards the producer is expanded over, the consumer is already one instance per value of `{wc}`, so that value is fixed for the instance and the gather ranges only over the producer's other wildcards. On a producer expanded over `{p}` x `{k}`, two values each and so four outputs:

| Consumer | Instances | Each receives |
|------------------------|------------------------|------------------------|
| the `_aggr` alone | 1 | all 4 |
| two parameters, `new_object_exp("{p}")` and the `_aggr` | 2, one per `p` | its own 2, the `k` values of its `p` |
| two parameters, `new_object_exp("{p}")` and `_aggr("{{po}}", by = "p")` | 2, one per `p` | the same as above |

So `by` is written when the consumer is not already expanded over the grouping wildcard. When it is, `by` has nothing left to fix and the last two rows coincide.

A static `{wc}` in the aggregated pattern does not, by itself, pin anything. On a consumer with no expansion driver it is an ordinary aggregation axis. With a producer expanded over `{sample}` = s1,s2,s3, each emitting `out/{sample}.txt`:

``` r
input = new_input(files = new_dep("greet", new_file_aggr("out/{sample}.txt")))
# -> one consumer instance, receiving the list of all 3 files.
```

`{wc}` in the pattern becomes a per-instance filter only when some other parameter already expands the consumer over `{wc}`. The pattern is then resolved against that instance's value and selects the matching subset. Adding `new_object_exp("{sample}")` to the consumer above turns the very same dep into a filter: each instance then receives only its own `out/<sample>.txt`. No `{{dynamic}}` token is required for this. Section 5.3.1 describes the richer form (`{wc}` prefixed to a producer's dynamic pattern, `outs/{sample}_{{any}}.ext`), which is the same mechanism applied to a producer whose file names are only known at runtime.

When `by` cannot apply, that is when the grouping or pairing key is not a wildcard the producer shares, for example a matched IP and input control keyed by filename, or a sample-sheet grouping, do not force it. Aggregate everything with `new_*_aggr` (no `by`, section 7.2) into one consumer and perform the selection and pairing in the consumer body, keying on `basename()`. The same approach covers matched case and control, N-way comparisons and sample-sheet-driven grouping.

A complete, runnable workflow using everything in this section is section 7.6.

#### 5.3.1 Per-sample input filter: `{wc}_{{any}}` on a consumer dep

A consumer expanded over a static wildcard `{wc}` can fetch only the subset of a producer's dynamic-output files that match its own binding, instead of the whole folder. Add the consumer's static wildcard as a filename prefix on top of the producer's `{{dynamic}}` pattern.

The example below is written for shell tasks, whose backticks mark the static directory prefix (section 4.4.2). For an R or Python task, write the same patterns without backticks: `new_file("outs/{{any}}.fastq.gz")` and `new_file_aggr("outs/{sample}_{{any}}.fastq.gz")`. The filter itself is the same either way, since the selection is performed by the `{sample}_` prefix and not by the backticks.

``` r
# producer (one instance) emits all files:
output = new_output(fastqs = new_file("`outs/`{{any}}.fastq.gz"))

# consumer (expanded over {sample}) fetches only its own:
fastqs = new_dep("mkfastq",
  new_file_aggr("`outs/`{sample}_{{any}}.fastq.gz"))
#                       ^^^^^^^^  count_85 -> only 85_* ; count_86 -> only 86_*
```

When the consumer runs on a different worker than the producer, only the matching subset is transferred P2P rather than the whole output, which gives cross-node parallelism without moving the full dataset. Co-located consumers also receive only their subset.

Flumen compares the consumer dep pattern with the producer output pattern. The consumer is accepted as a specialization when it has the same directory, the same `{{dynamic}}` token and the same suffix after it, plus an extra static `{wc}` before the `{{dynamic}}`. That extra prefix, with the binding substituted, for example `85_`, becomes an anchored basename regex (`^85_.*$`) which the producer applies before sending. The identifier is matched as literal text, so a value holding a regex metacharacter is safe: `CD4+`, `PD-1+` and `S1.a` match themselves and nothing else. Backticks are stripped for the filter and kept only for the shell substitution of `{name}` by the directory prefix.

**Requirements**:

-   The consumer must be expanded over `{wc}` (e.g. `new_object_exp("{wc}")` in another input) so `{wc}` is bound per instance.

-   The static `{wc}` must sit before the `{{dynamic}}`, as a filename prefix.

-   The separator (`_`) prevents partial matches: for sample `1` the filter `1_*` does not match `10_...`, and `Undetermined_*` is excluded by a `{sample}_` filter.

-   When one identifier is a prefix of another up to the separator, the longest match wins. The separator alone does not resolve that case: with samples `A` and `A_1`, the file `A_1_v.txt` satisfies `^A_.*$` as well, so the filter also excludes any prefix belonging to a more specific sibling. `A` is given `^(?!A_1_)A_.*$` and receives only its own files, while `A_1` keeps `^A_1_.*$`. This is what makes the `donor` / `donor_rep` naming safe. The exclusion is generated only when a more specific sibling actually exists, so an ordinary cohort is filtered exactly as before.

-   Assumption this mechanism rely on: a produced file belongs to the sample whose identifier its name starts with. That is the only link available, because a producer such as `cellranger mkfastq` emits every sample's files in one task and nothing records which file belongs to which sample, so the naming convention is the attribution. A tool that names its outputs without the sample id cannot be filtered this way; give each sample its own output directory instead.

Within this mechanism, that is for a producer whose output pattern is dynamic (`{{any}}`), a consumer pattern equal to the producer's carries no extra static `{wc}`, so there is no filter and the whole output is delivered. Existing workflows are unaffected. This does not generalise to a producer whose pattern already contains the static `{wc}` (e.g. `parts/{sample}_{ref}.txt`): there an expanded consumer filters even when its pattern is identical (section 5.3).

Current syntax limitations (the static `{wc}` must be a filename prefix):

| Form | Supported | Note |
|------------------------|------------------------|------------------------|
| `{wc}_{{any}}.ext` (prefix) | Yes | the cellranger fastq case |
| `{{any}}_{wc}.ext` (suffix) | No | the suffix after `{{dynamic}}` differs, so it is rejected as a mismatch |
| `{wc}/{{any}}.ext` (sub-dir) | No | wildcard in the path, not the basename prefix |
| `pre_{wc}_{{any}}.ext` (mid-name) | Yes | the filter is built from the whole resolved basename, so literal text before, after or between `{wc}` and the `{{dynamic}}` is allowed |
| `{wc1}_{wc2}_{{any}}.ext` (two filters) | Yes, if the consumer expands over both | each binding is substituted before the filter is built, so two wildcards make a longer literal prefix: an instance of `{sample}_{lane}_` receives only its own file. A wildcard the consumer does not expand over stays a token and becomes `.*`, which widens the filter with no warning: a consumer expanded over `{sample}` alone receives both groups |

#### 5.3.2 Broadcast dep from a single-instance producer to an expanded consumer

A producer that does not trigger expansion (no `new_object_exp()` / `new_file_exp()` in its inputs) stays single-instance. Two shapes are legitimate:

1.  Producer references no wildcard at all, so it is single by construction, as in `input = new_input(seed = 42L)`.
2.  Producer references a wildcard `{x}` via the wrapped vector-binding form `new_object("{x}")` (no `_exp`), giving a single instance with vector binding, where the function receives the whole vector of `{x}` values as a parameter (cf. section 1.1).

In both cases, when an expanded consumer (with `new_object_exp("{x}")`) declares `new_dep("P", value = ...)` on the single producer, Flumen broadcasts: the single producer instance becomes the parent of every consumer instance. The consumer waits for the producer to finish, then each instance reads the same producer output.

``` r
w_list <- new_wildcard_list(sample = c("s1", "s2", "s3"))

# Producer: explicit vector binding (sees all 3 samples at once), 1 instance.
P <- new_R_task(
  task_id = "P",
  input   = new_input(s = new_object("{sample}")),     # wrapped, no _exp
  output  = new_output(out = new_file("p_out.txt")),
  exec    = new_script('f <- function(s) writeLines("done", "p_out.txt")')
)

# Consumer: 3 instances; each depends on P (broadcast).
C <- new_R_task(
  task_id = "C",
  input   = new_input(
    sample = new_object_exp("{sample}"),
    p_file = new_dep(task_id = "P", value = new_file("p_out.txt"))
  ),
  output  = new_output(out = new_object("{{out}}")),
  exec    = new_script('f <- function(sample, p_file) readLines(p_file[[1]])[1]')
)
```

The broadcast is silent (no warning emitted) by design, mirroring the no-wildcard-ref case which has always broadcast silently.

#### 5.3.3 Memory bounds of aggregation

This section describes how the bytes are managed when the outputs of N producers converge on a single consumer. The result cache spills, while the delivery path does not. Every supervisor keeps a result cache, sized by `new_worker(cache_ram_limit = <MB>)`, with a RAM slot and a disk slot under `<cache_dir>/result_cache/`, one `<key>.bin` per entry. It holds both outputs produced locally and outputs received from other workers over P2P. When the RAM slot is full, entries are written to disk (LRU eviction). Spilling keeps the cache within its limit. It does not let an aggregate exceed the consuming worker's RAM.

Producer side is bounded per entry. A peer requests one producer's output per message so a worker holding many spilled outputs re-reads them one at a time. Its peak is the largest single entry rather than the sum so a single-threaded supervisor cannot hold two served payloads at once.

Consumer side is bounded by the sum. As each dependency resolves, its values are copied out of the cache and into the parameters the supervisor is assembling for the task, where nothing spills to disk. By the time the task is dispatched the whole aggregate is held in RAM in serialised form, and the wrapper then materialises it again as live R objects.

`chunk_size` does not bound memory. It caps the size of a wire message and not the sender's footprint, since a spilled entry is read back in full before the first chunk leaves.

The practical consequence is that no setting lets an object aggregation exceed the consuming worker's RAM, neither `cache_ram_limit` nor `chunk_size` nor the spill. What removes the ceiling is changing the contract to `new_file_aggr()` and streaming the files:

``` r
transpose_all <- function(files) {
  for (f in files) {
    m <- readRDS(f)
    saveRDS(t(m), sub("\\.Rds$", "_t.Rds", basename(f)))
    rm(m); gc()
  }
}
```

What stays live is then one matrix and its transpose, whatever N is, and the bound moves to the disk of the worker running the consumer. If that disk is the constraint in turn, keep the files where they are with `pre_staged()`, or declare a shared mount with `run_workflow(shared_path = ...)`.

### 5.4 Depending on several outputs of the same parent

A dependency names one output. Two forms that are rejected at build time:

``` r
# REJECTED: several values
new_input(pair = new_dep("A", new_file("out1.txt"), new_file("out2.txt")))

# REJECTED: one value carrying several paths
new_input(pair = new_dep("A", new_file(c("out1.txt", "out2.txt"))))
```

The vector form of `new_file()` is an input construct (scenario F4 in section 5.5, "input only"), not a dependency. Accepting it made the delivered set depend on how many paths the dependency happened to name: a dependency naming two of a producer's six outputs received all six, in the value of its own parameter and not merely as extra files on disk, so a script looping over its dependency processed outputs no consumer had requested.

Write instead one input parameter per output, each with its own `new_dep()`. Each one delivers exactly what it names:

``` r
new_input(
  first  = new_dep("A", new_file("out1.txt")),
  second = new_dep("A", new_file("out2.txt"))
)
```

When the names form a pattern rather than a fixed list, depend on the pattern with `new_file_aggr()` (scenarios F1, F2 and F5 in section 5.5), which is also the only form that works when the number of files is decided at runtime.

### 5.5 Multi-file output patterns

There is no dedicated folder constructor. Every "this task writes a directory of files" pattern is expressible via `new_file()` / `new_file_aggr()`.

Five situations cover the cases, each with the declarations it takes on the producer and on the consumer. Two of them are input only, where the files already exist and no task in the workflow writes them.

In the cells, backticks mark the static part of a pattern and are shell-only (section 4.4.2), so a `new_R_task` or `new_python_task` parameter carrying one is rejected at build time. The scenarios name shell tools, and in an R or Python task the same patterns are written without backticks, `new_file("idx.{{N}}.bt2")` for F1 or `new_file("outs/{{any}}.txt")` for F2. The dynamic wildcard does the same work either way.

| Scenario | Output declaration on producer | Consumer declaration |
|------------------------|------------------------|------------------------|
| F1 Pattern-deterministic multi-file, as when `bowtie2-build` produces `idx.1.bt2`, `idx.2.bt2` and so on | `` new_file('`idx`/{{N}}.bt2') `` or `` new_file('`idx`.{{N}}.bt2') `` | `` new_dep(task_id="prod", value = new_file_aggr('`idx`.{{N}}.bt2')) `` |
| F2 Runtime-arbitrary names, as produced by `cellranger` or `fastqc`, whose filenames are unknown in advance | `` new_file('`outs/`{{any}}.txt') `` | `` new_dep(task_id="prod", value = new_file_aggr('`outs/`{{any}}.txt')) `` |
| F3 Pre-existing reference directory (genome index, db) | input only, `new_input(ref = pre_staged(new_file(abs_dir)))` | n/a (consumed in place by the tool) |
| F4 Explicit multi-file vector (known at build time) | input only, `new_file(c("a.fa", "b.fa", "c.fa"))` | n/a |
| F5 Full output bundle of A consumed by B (multiqc / archival) | producer: `` new_file('`outs/`{{any}}') `` | `` new_dep(task_id="A", value = new_file_aggr('`outs/`{{any}}')) `` |

**Pitfall**: the path you declare in `new_output()` must equal where your `exec` actually writes. Flumen does not redirect the tool's output, and the declaration is only matched against the files the script produces. Choosing a convenient output directory name when the tool writes to a fixed path is a bug: `cellranger mkfastq` always writes under `<id>/outs/fastq_path/`, `cellranger count` under `<id>/outs/`, and that path is the one to declare, not one chosen for convenience.

## 6. Running across machines

In a single machine, the scheduler spawns the workers and communicates to them over IPC, and peer transfer between two local workers is IPC as well. In a multi-machine setting, every remote supervisor connects outwards to the master's inbound listen sockets. Those sockets are the pool given to `run_workflow(ports = ...)`. The master and the peers dial a worker. A worker's own output is fetched from it, so its data-request socket must be reachable from the master and from any worker that consumes what it produces. That socket, and the transfer pool behind it, are the range given to `new_worker(ports = ...)`. Both directions have to be open for a workflow that moves data across hosts.

### 6.1 Launcher constructors

``` r
ssh_launcher(host_user, host_port = 22L, identity = NULL,
             cwd = NULL, R_cmd = "Rscript", resources = NULL)   # R/launcher_ssh.R

start_worker(master_url, worker_id, self_host = NULL)           # R/remote_worker.R
```

-   `ssh_launcher`:
    -   `host_user`: `"user@host"` (required).
    -   `host_port`: the SSH port on that host, default `22`.
    -   `identity`: path to the SSH private key, such as `"~/.ssh/id_ed25519"`. `NULL`, the default, leaves the choice to SSH itself, which tries the agent and then the default key files.
    -   `cwd`: remote working directory for the R session. If `NULL`, falls back to `worker$root_path`. Leave it `NULL`, or set it to the same directory as `root_path`: a worker running elsewhere places files inconsistently (section 6.4).
    -   `R_cmd`: the `Rscript` executable on the remote host, default `"Rscript"`, which requires it on the remote `PATH`. Set it to an absolute path when R is installed at a non-standard location. This is the launcher's own copy of the setting: it starts the worker's R session, whereas `new_worker(R_cmd = ...)` is the interpreter that worker then uses for R tasks.
    -   `resources`: declares what the worker offers, matched against what a task requests in `requires`. Same schema, including `tags` capability labels.
-   `start_worker(master_url, worker_id)`: called on the remote host in manual mode, where the worker dials the master. `master_url` is `"tcp://master_ip:port"`, and `worker_id` must match the `name` of the corresponding `new_worker(...)` on the master side. `self_host` overrides the address this worker publishes for peers to dial; `NULL` auto-detects it.

### 6.2 Ports and reachability

Two separate ranges are used different machines.

-   `run_workflow(ports = ...)` is the master's pool of inbound sockets. `ports[1]` is the shared notify port, where all remote supervisors PUSH-dial, providing the fan-in for results, READY and file-received messages. Two further consecutive ports are taken for each remote worker, one for control dispatch and one for the terminate signal, so a workflow with `n` remote workers needs `length(ports) >= 1 + 2*n`. Default `61000:61099`. Local workers do not consume from this pool, since the master reaches a local supervisor over IPC.

-   `new_worker(ports = ...)` is that worker's own range for peer-to-peer transfer. `ports[1]` is the data-request listen socket, open for the whole workflow and dialled by the master and by peer workers. `ports[2:N]` form the transfer pool, cycled and reused across transfers and never growing beyond the range. Default `61100:61199`, the same for every worker. A pool too small for the worker's concurrency aborts the workflow with an explicit error.

In an all-local workflow the worker range has no effect and needs no override. No TCP endpoint is opened, so any number of local workers may keep the identical default range, as section 7.1 does with three of them. The overlap check applies only with TCP, that is when two workers that actually open TCP endpoints share a host, either a remote or hand-started worker co-located with another or with the master's listen pool. Their ranges must then be disjoint, or `run_workflow()` stops with `overlapping TCP port ranges: '<a>' A..B and '<b>' C..D`.

At start `run_workflow()` prints a port plan listing the master listen ports and, for each worker host that must accept peer-to-peer traffic from another host, that worker's data-request port and its pool. Workers that share a host with their peers need no inbound rule between them, so their range does not appear. Check the firewall against that plan before executing the workflow.

### 6.3 A mixed local and remote workflow

Local workers must not set self_host. In a mixed setup the framework injects self_host = master_host into every local worker automatically, and `new_worker()` rejects a user-set `self_host` on a local worker. If a local worker must serve data to a remote consumer, that is when it produces an output a remote task fetches, give it `ports = <contiguous range>` for its P2P endpoint.

``` r
w_local <- new_worker(
  name              = "local1",
  n_concurrent_task = 3,
  ports             = 61200:61249   # it serves the producer's files to the remote consumer
  # No self_host: injected from master_host (see the note above).
)

w_remote <- new_worker(
  name              = "remote1",
  self_host         = "<remote_ip_or_hostname>",   # reachable from master and peers
  root_path         = "/path/on/remote",           # working dir on remote
  n_concurrent_task = 1,
  ports             = 61100:61199,                 # P2P range open on the remote's firewall
  launcher          = ssh_launcher(
    host_user = "<user>@<remote_ip_or_hostname>",
    resources = list(gpu = 1)
  )
)

# Runs on the local worker, and its files move to the remote consumer.
produce <- new_R_task(
  task_id = "produce",
  input   = new_input(n = 3),
  output  = new_output(out = new_file("data_{{out}}.tsv")),
  exec    = new_script('
    write_shards <- function(n) {
      for (i in seq_len(n))
        write.table(data.frame(x = i), paste0("data_", i, ".tsv"),
                    sep = "\t", row.names = FALSE)
      NULL
    }
  ')
)

# A task with requires = list(gpu = 1) gets pinned to w_remote
consumer <- new_R_task(
  task_id  = "consume",
  input    = new_input(
    fs = new_dep(task_id = "produce",
                 value   = new_file_aggr("data_{{out}}.tsv"))
  ),
  output   = new_output(res = new_object("{{out_C}}")),
  exec     = new_script('
    read_all <- function(fs) {
      paths <- if (is.list(fs)) unlist(fs, use.names = FALSE) else fs
      lapply(paths, function(p) read.table(p, header = TRUE, sep = "\t"))
    }
  '),
  requires = list(gpu = 1)
)

workflow <- new_workflow(
  workflow_id = "mixed",
  desc        = "local producer, GPU consumer on the remote worker",
  task_list   = list(produce, consumer)
)

run_workflow(
  workflow,
  worker_list = list(w_local, w_remote),
  chunk_size  = 1e8,
  master_host = "<master_ip_or_hostname>",   # reachable from remote
  ports       = 61000:61099,                   # master's per-remote-worker listen pool
  log_dir     = "log"
)
```

Two workers are declared, one local with three concurrent slots and one remote reached over SSH, with its own working directory and port range on the other machine. The remote worker advertises a GPU among its resources, and the consumer task requests a GPU through requires, so the scheduler can place that task only there. The task itself gathers every file the producer wrote with `new_file_aggr()` and reads them all. The call to `run_workflow()` specifies the address of the master and a pool of ports for the traffic between the master and the remote workers.

### 6.4 Manual startup of a worker (no SSH)

A manually started worker that will run on another machine needs a launcher.

Do not leave `launcher = NULL`. A worker without a launcher is a local worker, the scheduler spawns it itself, prints `[flumen] port plan: local workflow - IPC sockets`, and the manually-started process is ignored.

``` r
# master side
manual_launcher <- function(resources = NULL)
  structure(list(resources = resources), class = "manual_launcher")
launch_worker.manual_launcher <- function(config, worker) invisible(TRUE)

w <- new_worker(name = "remote1", self_host = "<worker_ip>",
                n_concurrent_task = 2,
                launcher = manual_launcher(resources = list(tags = "linux")))

run_workflow(workflow, worker_list = list(w), chunk_size = 1e8,
             master_host = "<master_ip_or_hostname>",
             ports = 61000:61099)
```

`run_workflow()` then prints the port plan and blocks on `startup_timeout` (default 30s) waiting for the worker to dial in:

```         
[flumen] port plan - inbound TCP ports to open

  192.168.1.10     master (scheduler)
       61000-61002    from 10.0.0.7

  control ports: notify 61000 shared; remote1 61001-61002
  example port rule: ufw allow from 10.0.0.7 to any port 61000:61002 proto tcp
```

Remote side, in any R session. `master_url` needs two ports, the shared notify port and that worker's control port, comma-separated and in that order, exactly as the port plan above lists them:

``` r
library(flumen)
start_worker(master_url = "tcp://<master_ip_or_hostname>:61000,61001",
             worker_id  = "remote1",
             self_host  = "<worker_ip>")
```

A single port is rejected with `init_sockets(): malformed master_url ... Expected 'tcp://host:notify_port,ctrl_port'`.

`worker_id` must match the `name` of the matching `new_worker()` on the master side.

Start the session in the directory declared as `root_path`. `ssh_launcher()` does this automatically, since its `cwd` defaults to the worker's `root_path`, whereas a hand-started worker, or one started by a custom launcher, inherits the directory its R session was started in.

``` r
setwd("/data/flumen_run")          # == the worker's root_path
start_worker(master_url = "tcp://10.0.0.1:61000,61001",
             worker_id  = "remote1", self_host = "10.0.0.7")
```

### 6.5 Localhost test pattern

The following is a pattern for testing a workflow on a single machine as if it involved a remote worker. It lets a generated workflow be verified before the deployment.

``` r
spawn_localhost_worker <- function(worker) {
  log_path <- file.path(tempdir(),
                        paste0("flumen_test_", worker$name, ".log"))
  ## the worker must run in its root_path, as ssh_launcher() arranges with `cd`
  expr <- sprintf(
    'setwd("%s"); flumen::start_worker(master_url = "%s", worker_id = "%s", self_host = "127.0.0.1")',
    worker$root_path, worker$master_url, worker$name
  )
  sys::exec_background("Rscript",
                       args    = c("-e", expr),
                       std_out = log_path,
                       std_err = log_path)
}

worker_root <- tempfile("flumen_test_root_")
dir.create(worker_root, recursive = TRUE)

w <- new_worker(
  name              = "w_test",
  self_host         = "127.0.0.1",
  root_path         = worker_root,
  n_concurrent_task = 1L,
  launcher          = spawn_localhost_worker     # plain function, not ssh_launcher
)

run_workflow(
  workflow,
  worker_list = list(w),
  chunk_size  = 1e7,
  master_host = "127.0.0.1",
  ports       = 6800:6819                         # any free contiguous range in [1024, 65535]
)
```

What this exercises that an all-local worker (`launcher = NULL`) does not:

-   The full TCP handshake path, in which the worker dials the master and registers.
-   Per-worker `master_url` allocation, where the master takes `1 + 2*n` ports from `run_workflow(ports = ...)`, namely `ports[1]` as the shared notify port plus two per remote worker for control and terminate.
-   Per-worker P2P port pool (`new_worker(ports = ...)`), comprising the data-request listen socket and the transfer pool, sent to the worker through the handshake.
-   Resource matching when `worker$launcher$resources` is involved. It is `NULL` here, but a launcher declaring resources may be passed instead.

## 7. Minimal examples

### 7.1 R workflow, 4 tasks with multi-wildcard expansion + aggregation

``` r
library(flumen)

w_list <- new_wildcard_list(
  new_wildcard("dataset",   list(ob1 = data.frame(x = 1:10, y = 1:10))),
  new_wildcard("dataset_2", list(ob2 = data.frame(x = 1:5,  y = 1:5))),
  new_wildcard("dataset_3", list(ob3 = data.frame(x = 1:2,  y = 1:2)))
)

A <- new_R_task(
  task_id = "A",
  new_input(df = new_object_exp("{dataset}")),
  new_output(out = new_object("{{out}}")),
  exec = new_script('
    generate_df <- function(df) return(list(df))
  ')
)

B <- new_R_task(
  task_id = "B",
  new_input(df = new_object_exp("{dataset_2}")),
  new_output(out = new_object("{{out_2}}")),
  exec = new_script('generate_df <- function(df) return(df)')
)

C <- new_R_task(
  task_id = "C",
  new_input(df = new_object_exp("{dataset_3}")),
  new_output(out = new_object("{{out_3}}")),
  exec = new_script('generate_df <- function(df) return(df)')
)

D <- new_R_task(
  task_id = "D",
  new_input(
    "df"   = new_dep(task_id = "A", value = new_object_exp("{{out}}")),
    "df_2" = new_dep(task_id = "B", value = new_object("{{out_2}}")),
    "df_3" = new_dep(task_id = "C", value = new_object("{{out_3}}"))
  ),
  new_output(out = new_object("{{out_4}}")),
  exec = new_script('
    generate_df <- function(df, df_2, df_3) return(list(df, df_2, df_3))
  ')
)

workflow <- new_workflow(
  workflow_id   = "demo",
  desc          = "Multi-wildcard with aggregation in D",
  task_list     = list(A, B, C, D),
  wildcard_list = w_list
)

worker_list <- list(
  new_worker(name = "w1", n_concurrent_task = 1),
  new_worker(name = "w2", n_concurrent_task = 1),
  new_worker(name = "w3", n_concurrent_task = 1)
)

run_workflow(workflow, worker_list = worker_list, chunk_size = 1e8)
```

Three wildcards each hold one data frame. Tasks A, B and C are independent, one per data frame, and each returns its own result. Task D depends on all three at once, receives them as three separate arguments, and returns them together in a single list. Each of the three workers runs one task at a time, so A, B and C can execute in parallel and D starts as soon as they are all done.

### 7.2 Aggregation (list passthrough)

``` r
w_list <- new_wildcard_list(
  dataset = list(ob1 = data.frame(x = 1:10, y = 1:10))
)

A <- new_R_task(
  task_id = "A",
  input  = new_input(df = new_object_exp("{dataset}")),
  output = new_output(out = new_object("{{out}}")),
  exec   = new_script('
    generate_df <- function(df) return(list(df, df))
  ')
)

B <- new_R_task(
  task_id = "B",
  input  = new_input(
    dfs = new_dep(task_id = "A", value = new_object_aggr("{{out}}"))
  ),
  output = new_output(out = new_object("{{out_B}}")),
  exec   = new_script('
    generate_df <- function(dfs) return(dfs)
  ')
)

workflow <- new_workflow(
  workflow_id   = "aggr_demo",
  desc          = "A returns list; B aggregates",
  task_list     = list(A, B),
  wildcard_list = w_list
)

run_workflow(workflow,
             worker_list = list(new_worker(name = "w1", n_concurrent_task = 1)),
             chunk_size  = 1e8)
```

One wildcard holds a single data frame. Task A runs once and returns a list of two data frames, which flumen reads as two outputs. Task B aggregates both with `new_object_aggr()` and receives them as one list, which it passes through unchanged. A single worker runs the two tasks in turn, so B starts once A has finished.

#### 7.2.1 Keeping element identity through an aggregation

An object aggregation gives the consumer no way to distinguish its elements by position (section 3.2.3). Have the producer name its value, and the name moves with it:

``` r
A <- new_R_task(
  task_id = "A",
  input  = new_input(s = new_object_exp("{sample}")),
  output = new_output(out = new_object("{{out}}")),
  exec   = new_script('
    make <- function(s) setNames(list(compute(s)), paste0("s", s))   # named
  ')
)

B <- new_R_task(
  task_id = "B",
  input  = new_input(ms = new_dep(task_id = "A", value = new_object_aggr("{{out}}"))),
  output = new_output(out = new_object("{{out_B}}")),
  exec   = new_script('
    use <- function(ms) ms[["s1"]]        # safe;  ms[[1]] is not sample 1
  ')
)
```

It must be a named `list` and not a named vector. `setNames(list(v), id)` names the element inside the aggregate, which is what makes `ms[["s1"]]` work, while `setNames(v, id)` leaves `names(ms)` empty and the lookup returns `NULL` with no error.

A multi-element return is spliced into the aggregate, not nested. Two producers each returning `list(a = 1, b = "x")` give four elements with the names repeated `a, b, a, b`, not two lists of two. Return one value per instance when each instance must stay one unit.

### 7.3 Shell pipeline with a dynamic output

In the following example, the backticks in `` `index/idx/idx_base` `` mark the static portion of a path around the dynamic placeholder `{{N}}`, which stands for the `.bt2` files `bowtie2-build` produces, whose number is not known in advance.

``` r
w_list <- new_wildcard_list(
  ref    = c("ref1"),
  sample = c("s1", "s2", "s3")
)

op1 <- new_shell_task(
  task_id = "indexing",
  input  = new_input(
    "--threads" = 10,
    new_file("../genome/{ref}.fa")
  ),
  output = new_output(
    new_file("`index/idx/idx_base`.{{N}}.bt2")
  ),
  exec = new_cmd("bowtie2-build")
)

op2 <- new_shell_task(
  task_id = "alignment",
  input  = new_input(
    "threads"   = 10,
    "genome"    = new_dep(task_id = "indexing",
                          value = new_file("`index/idx/idx_base`.{{N}}.bt2")),
    "reads"     = new_file_exp("../fastq/{sample}.fastq"),
    "genome_fa" = new_file("../genome/{ref}.fa")
  ),
  output = new_output("sam" = new_file("sam/{sample}.sam")),
  exec   = new_script('
    #!/bin/bash
    bowtie2 -p {threads} -x {genome} -U {reads} -S {sam}
  '),
  requires = list(gpu = 1)
)

workflow <- new_workflow(
  workflow_id   = "rnaseq",
  desc          = "RNA-seq alignment",
  wildcard_list = w_list,
  task_list     = list(op1, op2)
)
```

Two wildcards describe the run, one reference genome and three samples. The indexing task calls `bowtie2-build` once and writes an index made of several files whose number is not known in advance, which is what the dynamic placeholder `{{N}}` stands for, with the backticks marking the static part of the path. The alignment task then runs once per sample, receiving the whole index as its genome argument together with that sample's fastq file, and writes one sam file per sample. It also requests a GPU, so it runs only on a worker that offers one.

### 7.4 Polyglot workflow: from R to Python

``` r
sample_list <- setNames(
  lapply(1:6, function(i) data.frame(x = 1:10, y = rnorm(10))),
  paste0("s", 1:6)
)
w_list <- new_wildcard_list(sample = sample_list)

A <- new_R_task(
  task_id = "A",
  input  = new_input(df = new_object_exp("{sample}")),
  output = new_output(out = new_object("{{out_A}}")),
  exec   = new_script('
    generate_df <- function(df) {
      data.frame(x = rnorm(2e6), y = rnorm(2e6), z = rnorm(2e6))
    }
  ')
)

B <- new_python_task(
  task_id = "B",
  input  = new_input(
    df = new_dep(task_id = "A", value = new_object("{{out_A}}"))
  ),
  output = new_output(out = new_object("{{out_B}}")),
  exec   = new_script('
    def process_df(df):
        return df.__class__({
            "n_rows": [len(df)],
            "mean_x": [float(df["x"].mean())],
            "mean_y": [float(df["y"].mean())],
            "mean_z": [float(df["z"].mean())],
        })
  ')
)

workflow <- new_workflow(
  workflow_id   = "polyglot",
  desc          = "R producer -> Python consumer",
  task_list     = list(A, B),
  wildcard_list = w_list
)
```

Six samples drive the expansion, so task A runs six times. The dep uses the plain `new_object()`, which pairs each B instance with the A instance of the same `{sample}`. Declaring B's dep with new_object_exp() instead would create one B per object A returns, not one per instance of A. Here the two counts agree only because each A instance returns a single object; if A returned two, the six instances would become twelve. A is written in R and returns a large data frame of three columns and two million rows. Task B is written in Python and receives that data frame as a pandas `DataFrame`, returning a one row summary with the row count and the mean of each column. The object crosses between the two languages over the wire, with no file in between. The second example shows the same crossing in the opposite direction, where a Python task builds a pandas `DataFrame` and an R task consumes it as a real `data.frame`:

``` r
build <- new_python_task(
  task_id = "build", input = new_input(n = 5L),   # 5L, not 5 — see the note below
  output  = new_output(tbl = new_object("{{tbl}}")),
  exec    = new_script('
def build_table(n):
    import pandas as pd
    return pd.DataFrame({"gene": ["g%d" % i for i in range(n)],
                         "count": list(range(n))})
  ')
)
check <- new_R_task(
  task_id = "check",
  input   = new_input(tbl = new_dep("build", new_object("{{tbl}}"))),
  output  = new_output(o = new_file("check.txt")),
  exec    = new_script('
    f <- function(tbl) writeLines(paste(nrow(tbl), ncol(tbl),
                                        paste(colnames(tbl), collapse = ",")),
                                  "check.txt")
  ')
)
```

Two notes on this example. `pandas` is required on every Python worker, not only to build a DataFrame: an R `data.frame` reaches the body as a `pandas.DataFrame`, and the wrapper imports pandas whatever the task does (section 4.3). A `pyarrow.Table` return works the same way. And an R numeric literal crosses as a Python `float`: `new_input(n = 5)` makes `range(n)` raise `TypeError: 'float' object cannot be interpreted as an integer`. Declare integers as `5L`, or coerce with `int(n)` in the body.

### 7.5 Dynamic file expansion (one task generates N files at runtime)

``` r
A <- new_R_task(
  task_id = "A",
  output  = new_output(out = new_file("gen_{{out}}.Rds")),
  exec    = new_script('
    generate_df <- function() {
      n <- sample(1:10, 1)
      for (i in 1:n) {
        df_temp <- data.frame(id = 1:10, val = rnorm(10))
        saveRDS(df_temp, paste0("gen_", i, ".Rds"))
      }
    }
  ')
)

B <- new_R_task(
  task_id = "B",
  input  = new_input(
    "file" = new_dep(task_id = "A", value = new_file("gen_{{out}}.Rds"))
  ),
  output = new_output(out = new_object("{{out_2}}")),
  exec   = new_script('
    generate_df <- function(file) {
      lapply(file, readRDS)
    }
  ')
)

workflow <- new_workflow(
  workflow_id   = "dyn_files",
  desc          = "A writes N files; B reads them",
  task_list     = list(A, B)
)
```

Task A takes no input and writes a number of files decided at runtime, between one and ten, all matching the pattern `gen_{{out}}.Rds`. Task B depends on that pattern with a plain `new_file()` and so runs once, receiving every path in a single list, which lapply then reads.

**Hint**: `B` receives `file` as a `list` of N paths (one per output generated by A), which is why `lapply` is the natural way to process them. Any parameter that resolves to more than one file arrives as a list.

The variant that follows swaps the constructor for `new_file_exp()`, and that alone turns one instance holding N paths into N instances each holding one path. The producer `A` is unchanged, so the dep repeats `A`'s declared pattern verbatim:

``` r
B <- new_R_task(
  task_id = "process",
  input   = new_input(chunk = new_dep("A", new_file_exp("gen_{{out}}.Rds"))),
  output  = new_output(o = new_object("{{m}}")),
  ## chunk is a length-1 path: one of A's files. This task runs once per file.
  exec    = new_script('
    f <- function(chunk) readRDS(chunk)
  ')
)
```

The output of a task expanded at run time must be an object. Its instances all come from the one declaration, so a file output hands every one of them the same path, and neither shape of file output survives that:

-   A dynamic file output, `new_file("done/{{m}}.Rds")`, is refused by the scheduler as soon as the instance count is known, which is the first moment it can be: *"expanded into N instances at run time, and its dynamic file output 'done/{{m}}.Rds' carries nothing to tell them apart"*. A dynamic output is collected by matching its pattern in its directory around the body, so each instance would also claim whatever a sibling wrote while it ran.
-   A static file output, `new_file("done/all.Rds")`, is refused as well, so the instances never get to overwrite one another. On a task fanned out by a static wildcard the refusal comes at build time, where the count is known. On a task expanded at run time the workflow builds without complaint and the refusal comes from the scheduler, as soon as a second instance resolves to the same path: *"run time expansion: `<task>` (`<exe_id>`) and `<task>` (`<exe_id>`) both resolve to the file output 'done/all.Rds'. Fix: return an object output (new_object); instances created at run time have no wildcard value of their own to put in a file name."* The run stops there.

An object output is immune by construction, because it is kept per instance in the worker's result cache and never touches a shared directory. Reusing the producer's placeholder to distinguish the instances, `new_file("done/{{out}}.Rds")`, is refused for a different reason: a dynamic wildcard name must be unique within a task.

### 7.6 Partial aggregation (`by =`): fan out over two wildcards, merge per group

In the following example, `make_part` is expanded over both wildcards, so it runs once per `{sample}` x `{ref}` combination (4 instances). `merge` is expanded over `{sample}` only and aggregates over the remaining axis, so it runs twice and each instance receives just its own sample's files.

To notice:

-   merge takes `smp = new_object_exp("{sample}")` alongside the aggregated dep. The grouping does not need it. `by = "sample"` groups on its own, and without the \_exp you still get one instance per sample, each holding only its own two files. The reason is a different one. Output parameters are stripped from the call, so an input carrying the wildcard value is the only way the body can rebuild the name of its own output (section 4.1.3). Write it whenever the body has to name an output for its group.
-   Since the consumer is already expanded over `{sample}`, `by = "sample"` is redundant. The aggregation covers only the remaining axis either way (section 5.3). It is written here because it makes the intent explicit.

``` r
w_list <- new_wildcard_list(sample = c("s1", "s2"), ref = c("r1", "r2"))

make_part <- new_R_task(
  task_id = "make_part",
  input   = new_input(s = new_object_exp("{sample}"),
                      r = new_object_exp("{ref}")),
  output  = new_output(part = new_file("parts/{sample}_{ref}.txt")),
  exec    = new_script('
    write_part <- function(s, r) {
      n <- as.integer(substring(s, 2)) * 10L + as.integer(substring(r, 2))
      writeLines(paste(s, r, n),
                 file.path("parts", paste0(s, "_", r, ".txt")))
    }
  ')
)

merge_sample <- new_R_task(
  task_id = "merge",
  input   = new_input(
    smp   = new_object_exp("{sample}"),
    parts = new_dep(task_id = "make_part",
                    value   = new_file_aggr("parts/{sample}_{ref}.txt",
                                            by = "sample"))
  ),
  output  = new_output(merged = new_file("merged_{sample}.txt")),
  exec    = new_script('
    merge_parts <- function(smp, parts) {
      paths <- unlist(parts, use.names = FALSE)
      lines <- unlist(lapply(paths, readLines), use.names = FALSE)
      writeLines(sort(lines), paste0("merged_", smp, ".txt"))
    }
  ')
)

run_workflow(
  new_workflow("partial_aggr", "fan out over sample x ref, merge per sample",
               task_list = list(make_part, merge_sample), wildcard_list = w_list),
  worker_list = list(new_worker("w1", n_concurrent_task = 2))
)
```

**Result**: `merged_s1.txt` holds `s1 r1 11` / `s1 r2 12`, `merged_s2.txt` holds `s2 r1 21` / `s2 r2 22`, so each group sees its own two files and no others.

## 8. Common errors: quick reference

| Symptom | Cause | Remedy |
|------------------------|------------------------|------------------------|
| `'exec' must be created with new_script()` | A plain string passed to `exec` | Wrap it in `new_script('...')` |
| `the task declares neither input nor output` | Both `new_input` and `new_output` are NULL | Add at least one |
| `the DAG is disconnected` | The task graph is not connected, which covers an unconnected task and also two chains that are each complete but never reference each other | Add a `new_dep` between the parts, or run them as separate workflows. A single-task workflow is accepted |
| `dependency cycle in task_list, involving task_ids: '<id>', '<id>'` | A depends on B and B on A | Redesign the flow |
| `the dynamic wildcard '{{X}}' is used in N parameters (#...)` | Same `{{name}}` (e.g. `{{out}}`) reused on ≥2 parameters of one task (input dep + output, or two inputs) | Rename one occurrence, for instance the output `{{out}}` to `{{out2}}` |
| `wildcard(s) '<x>' used in task_list but not declared in wildcard_list` | A static `{x}` referenced in a task but missing from `new_wildcard_list(...)` | Declare it: `new_wildcard_list(x = c(...))` |
| `'requires': unknown field(s)` | A field outside `{cpu, mem, gpu, tags}` | Use only the allowed fields |
| `requires resources that no worker ... can satisfy` | Worker pool does not cover `requires` | Add a worker with adequate `resources`, or relax `requires` |
| `'self_host' is reserved for remote workers` | `self_host = ...` on a local worker (`launcher = NULL`) | Remove it (locals inherit from `master_host` of `run_workflow`) |
| Warning `input '<p>' is set to be transferred but does not exist on the master` | `new_file("literal/path")` transferred (default) but file missing on master | Stage the file on the master, or wrap the file input in `pre_staged()` if it is on the worker, or use a wildcard if the path is dynamic |
| Dynamic output not populated | The task declares several outputs and the returned value does not name them | Return a named list whose names are the output parameter names, e.g. `list(a = ..., b = ...)` for `new_output(a = ..., b = ...)`. With a single declared output, return the value directly, as the return-shape table in section 4.1.2 shows, where a one-element named list is unwrapped and its name dropped. |
| `task '<id>' did not produce declared output '<p>'` | Runtime check, emitted by the wrapper, which knows the `exe_id` and not the `task_id`: a declared static file output was not written by this run. | Make the tool write the declared path, or fix the path (cwd, prefix, file name). There is no optional output: do not declare a file the task may skip. See section 5.5. |
| `instance '<exe_id>' output '<p>' already exists: ...` | Runtime check, before the body, emitted by the wrapper, which knows the `exe_id` and not the `task_id`: a declared output (or a file matching a declared dynamic pattern) is already at its path, typically the previous run's. A task never overwrites: what it produced could not be distinguished from what was already there. | Remove the previous outputs or run in another directory. A file the task must find already in place is an input (`pre_staged()`, `shared_path`), not an output. See section 5.5. |
| `both write into '<dir>' with overlapping names ('<p1>' and '<p2>')` | Two instances (or tasks) resolve to dynamic patterns that can collect each other's files (`A_{{n}}.txt` also accepts `A_1_x.txt`). | Give each instance its own directory, or a head no other instance's head extends. See section 5.5. |
| `declares an ambiguous output path '<p>'` | `new_output(new_file("output_dir"))`, a single segment with no extension, wildcard or backtick. It is also raised on a trailing `/`, for example `"out/"`. | Add an extension or a wildcard. Shell task: `` new_file('`output_dir`/{{out}}') `` (backtick + dynamic). R/Python task, where backticks are illegal: `new_file("output_dir/{{out}}")`. See section 5.5. |
| `outputs #... are pure dynamic wildcards with no static discriminator` | Two or more outputs of one task are a `{{name}}` carrying no literal text, so a consumer's `new_dep` pattern cannot select one of them. One such output is the normal case and is accepted, while two or more are not. | Give each output static text and a distinct dynamic name, for example `new_object("res_{{a}}")` with `new_object("sum_{{b}}")`, or `new_file("dir_a/{{x}}.txt")` with `new_file("dir_b/{{y}}.txt")`. Reusing one `{{name}}` across both violates the uniqueness rule instead. Alternatively keep a single output slot and return a named list, aggregated with `new_object_aggr("{{out}}")` and indexed by name. |
| `new_dep(task_id = '<id>', value = '<p>') matches no output of '<id>'` | Build-time check: the `value` of `new_dep` does not match any output pattern of the producer (typo or producer rename). | Align the consumer dep value with one of the producer's outputs (`validation.R`). |
| `new_object_aggr('<p>') uses a static wildcard pattern` | Object aggregation groups by a dynamic name, and a static `{wc}` there matches no producer at runtime. | Use `new_object_aggr('{{name}}')` matching the producer's `new_object('{{name}}')`. For an aggregation over files, use `new_file_aggr('dir/{wc}.ext')` instead, which does accept a static wildcard. See section 3.2.3. |
| `<type> cannot be sent to a Python task over Arrow` | A literal value written in `new_input()` of a Python task has a type Arrow cannot carry (a list, a complex vector, `raw`, an S4 object). Only Python is affected, since an R or shell task receives the same value through qs2 and is never constrained. Dependencies are not judged here, because their type is known only once the producer has run. | Pass a type Arrow carries (atomic vector, matrix, data.frame), or write the value to a file and pass the file. A named list of scalars is usually a `data.frame` declared as a list. Before this check the workflow was built, dispatched and transferred, and failed inside the consumer's supervisor, naming the consuming task rather than the declared parameter. |
| `at most one dynamic wildcard per parameter, found N: ...` | More than one `{{name}}` in a single path or value. At most one dynamic wildcard is allowed per parameter. | Keep one `{{name}}` per parameter and split the rest onto their own outputs. |
| `output parameter #N uses new_object_exp() / new_file_exp() which is not allowed for outputs` | An `_exp` or `_aggr` constructor was used inside `new_output(...)`. | Declare the output with plain `new_object()` / `new_file()`: expansion follows the input automatically, and aggregation is a consumer-side gather that belongs inside `new_dep(value = ...)`. |
| `the task's exec body must END with the function to call` | The last top-level expression of an R body is not a function, typically because a statement was written after the definition (`saveRDS(g, ...)` on the last line). | Move such statements inside the function so the definition stays last. See section 4.2. |
| `task '<id>' has duplicate parameter name(s)` | Two parameters of one task share the same name (two inputs named `x`, or an input and an output). | Rename one. Note this is raised only on real names: several unnamed parameters are accepted, they are a shell task's positional arguments. |
| `[shell op X] cannot consume parameter 'Y' as data.frame` (or `matrix`/`array`/`raw`/`list with non-atomic elements`) | Shell task receives a complex object from an upstream R/Python task. Shell ops accept only files, atomic scalars, or atomic vectors. | Have the upstream task write the value to a `new_file()` (csv/tsv/rds) and let the shell op consume the file; or marshal as character. See section 4.4.4. |
| `'ports' must be a contiguous range of >= 2 values in [1024, 65535]` | `new_worker(ports=...)` or `run_workflow(ports=...)` received a non-contiguous range, a value \<1024 or \>65535, or fewer than 2 ports. | Pass a contiguous integer range, e.g. `61100:61199` (worker) / `61000:61099` (master). |
| `'ports' has N ports but K remote worker(s) need M` | `run_workflow(ports=...)` is too small, because the master needs `1 + 2*K` ports, one shared notify port plus two per remote worker. | Widen `run_workflow(ports = ...)`. |
| `overlapping TCP port ranges: '<a>' A..B and '<b>' C..D` | Two TCP-opening workers share a host, or one collides with the master's listen pool. See `ports` in section 3.4. It is never raised in an all-local workflow. | Shift one worker's `new_worker(ports = ...)` (or the `run_workflow(ports = ...)` pool) so the ranges are disjoint. |
| `peer-to-peer port pool (...) exhausted` | A worker's P2P transfer pool (`new_worker(ports=...)[2:N]`) is too small for the number of concurrent cross-worker transfers. | Widen `new_worker(ports = ...)` or lower `n_concurrent_task`. |
| `the remote working directory <dir> does not exist on <host>` (or ssh's own message, e.g. `ssh: connect to host <host> port 22: Connection refused`) | Pre-flight SSH check before launching a remote `ssh_launcher` worker: the launcher `cwd`, which defaults to the worker's `root_path`, is missing on the remote host, or SSH to the host failed. | Create the directory on the remote (`ssh <host> mkdir -p <dir>`), or pass a valid `cwd` or `root_path`. For the SSH variant, check the passwordless key and the reachability of the host. |

## 9. Inspecting a run

### 9.1 `exe_id` after expansion

After `new_workflow()`, every expanded task has an `exe_id` that is a UUID generated by Flumen, for every task type. There is no suffix and no structure and an id carries no information about the task or the sample it belongs to. To find an instance's exe_id, inspect `workflow$task_list[[k]]$exe_id` directly or use `print_workflow_commands(workflow)` or `flumen_status()`.

### 9.2 Utilities

``` r
to_log(msg, sender, log_file = paste0(sender, "_log.log"))     # R/utils.R
print_workflow_commands(workflow_obj)                          # R/methods.R
preview_workflow(workflow, worker_list = NULL,                 # R/preview.R
                 master_host = NULL, ports = 61000:61099)
plot(workflow)                                                 # R/methods.R
flumen_status(path = getwd())                                  # R/status.R
```

-   `to_log`: appends `msg` as a timestamped line, attributed to `sender`. Works inside tasks and subprocesses. `log_file` defaults to `<sender>_log.log`. Inside a run the file is written to the directory given to `run_workflow(log_dir = ...)`, which the local subprocesses inherit. A remote worker, or a call made outside a run, writes to `log/` under the current working directory instead.
-   `print_workflow_commands(wf)`: prints, per task, the resolved command and its bound parameters.
-   `preview_workflow(wf, worker_list)`: prints the plan the scheduler would follow, without running the workflow. Passing `worker_list` prints the port plan too, so a port-range error is detected here rather than at launch.
-   `plot(workflow)`: plot the DAG with igraph.
-   `flumen_status(path)`: reads the scheduler-persisted task table (`central_scheduler_task_table_log.tsv`) and returns it as a data frame (one row per task instance: `Exe_ID`, `Op_ID`, `Level`, `Worker`, `Status`). This make possible to inspect a run from another R session while it runs, or after it ends.
    -   `Exe_ID` is the instance's own id, a UUID with no structure to read (section 9.1).
    -   `Op_ID` is the `task_id` the instance was expanded from, so every instance of one task carries the same value.
    -   `Level` is the instance's level in the dependency graph, 1 for a task with no producer.
    -   `Worker` is the worker the instance was sent to, named from the moment it is sent and not only once it runs.
    -   `Status` is one of `ready` (waiting for a free slot), `dispatched` (sent, with no confirmation back yet), `execution` (the worker confirmed it is running), `finished`, `failed` (this instance raised the error) and `aborted` (still running when the run was stopped). Only `finished` means the declared output is complete, and an `aborted` instance may have written part of the results before it was killed.

### 9.3 Fault detection: the thresholds and how to change them

Flumen stops a run rather than waiting for ever when a part of it stops responding. Each detector answers a different question, and each one is a compromise between reacting quickly and never reacting to a healthy system. Each one is an environment variable read on the master when `run_workflow()` starts, so you set them in the shell that launches the run or with `Sys.setenv()` before the call.

``` r
Sys.setenv(FLUMEN_WORKER_SILENCE_SECS = 1800)
run_workflow(workflow, worker_list = list(w))
```

The master sends the six that govern a worker's own behaviour in the handshake, so one setting reaches every node whatever launcher started it, and setting them on a worker node has no effect. The master uses `FLUMEN_WORKER_SILENCE_SECS` and `FLUMEN_PROBE_DEADLINE_SECS` itself, since it is the master that watches the workers. `FLUMEN_ORPHAN_CONNECT_SECS` is the one exception, read on the worker node because it is the time that worker waits for the handshake and so cannot arrive in it. Values are in seconds unless stated.

| Variable | Default | What it watches |
|------------------------|------------------------|------------------------|
| `FLUMEN_HEARTBEAT_SECS` | 20 | How often a remote worker sends its keepalive. Every threshold below must stay well above it. Change it only to make a test shorter. |
| `FLUMEN_WORKER_SILENCE_SECS` | 600 | A worker that stopped answering while holding work. |
| `FLUMEN_SCHEDULER_SILENCE_SECS` | 600 | A worker whose scheduler stopped answering shuts itself down, instead of computing for a run no caller is collecting. |
| `FLUMEN_STALLED_TRANSFER_SECS` + `FLUMEN_STALLED_TRANSFER_CHECKS` | 600 / 5 | A fetch from a peer that stopped sending. Measured as no new chunks, not as elapsed time, so a slow but progressing transfer is never touched. |
| `FLUMEN_DEAD_WRAPPER_SECS` + `FLUMEN_DEAD_WRAPPER_CHECKS` | 30 / 5 | A task process that exited without delivering a result, the usual cause being the operating system's out-of-memory killer. |
| `FLUMEN_ORPHAN_CONNECT_SECS` | 45 | A worker that never reached the scheduler at all, for instance an incorrect `master_host`. It self-terminates. Read on the worker node and carried there by no launcher, so a worker started by `ssh_launcher()` keeps this default. |
| `FLUMEN_PROBE_DEADLINE_SECS` | 30 | Before dispatching, every worker tests whether it can reach its peers and reports back. This is how long the master waits for those reports, and a worker merely slow to answer is recorded as unreachable, which can stop the run. |

Setting any of the detectors to `0` disables it.

## 10. Decision trees for chatbots

### 10.1 Patterns rejected at build time

Do not generate these. The constructor to use instead is in the wrapping section, and the goal-to-pattern table opens the examples.

| Rejected pattern | Use instead |
|------------------------------------|------------------------------------|
| Unwrapped wildcards: `"{x}"` or `"{{x}}"` | `new_object("{x}")` / `new_object_exp("{x}")` / `new_object("{{x}}")` |
| Same `{{name}}` reused across ≥2 params of one task | Each dynamic name must be unique per task; rename one, for example the output `{{out}}` to `{{out2}}` |
| Static `{x}` used but not declared in `wildcard_list` | Add it: `new_wildcard_list(x = c(...))` |
| `_exp` or `_aggr` in `new_output(...)` | Plain `new_object()` / `new_file()` on outputs |
| ≥2 outputs with no static text, `"{{a}}"` and `"{{b}}"` | A consumer cannot select one of them: add static text and keep the dynamic names distinct (`res_{{a}}` / `sum_{{b}}`, `dir_a/{{x}}.txt` / `dir_b/{{y}}.txt`), or keep one output returning a named list. A single `"{{out}}"` is valid. |
| `_aggr` outside `new_dep()` with only a dynamic wildcard | Wrap it in `new_dep(task_id = "P", value = new_*_aggr("{{x}}"))`. A standalone `_aggr` is valid only with a static `{x}` wildcard, for file fan-in. |
| Empty string in `new_file("")` / `new_object("")` | Provide a non-empty pattern; an empty string is valid only in `new_input` |
| Unclosed or unbalanced backticks: `` "`prefix/{{any}}" `` | Close the pair: `` "`prefix/`{{any}}" `` |
| Backtick pair wrapping only wildcards: `` "`{{x}}`" `` | Backticks delimit static text: put literal characters inside |
| Backticks in a `new_R_task` / `new_python_task` parameter | Backticks are shell-only; use the form without them, `"dir/{{any}}.ext"`. R and Python receive the resolved file list either way, and the directory comes from `dirname(paths[1])` if needed. |
| Trailing slash output path: `new_file("path/")` | Either a file with an extension, or a backtick plus a dynamic pattern |
| A file output and an object output on one task | Split them: keep the file output on the producer and return the object from a small downstream task. With a file output the return value is ignored, so the object would never be captured (section 4.1.3). |

### 10.2 Choosing the right shell `exec`

| Situation | Use | Note |
|------------------------|------------------------|------------------------|
| Single tool, no shell logic | `new_cmd("toolname")` | Recommended: no bash, no quoting pitfalls |
| Several commands, piping, redirections, shell variables or conditionals | `new_script(...)`, opening the body with `set -euo pipefail` | One command per line, never a backslash before a newline (section 4.1.1) |
| Long shell source | `new_script(file = "scripts/run.sh")` | Contents read at call time |

### 10.3 Choosing the right `new_worker` setup

One machine: `new_worker(name, n_concurrent_task)` with `launcher = NULL`, the default. A remote host reachable over passwordless SSH: add `root_path` and `launcher = ssh_launcher(host_user = "u@h")`. A host behind NAT or a firewall, or without an SSH server: set `self_host`, give it a launcher whose `launch_worker()` does nothing, and start the worker manually on that host.

`launcher = NULL` never means remote: it makes the worker local, and the run silently happens on the master. Section 6.1 has the launcher definition, section 6.2 the two ports a hand-started worker needs, and section 6.4 the working directory it must be started in.

### 10.4 Choosing whether to transfer a file

A file on the master that must reach the worker is declared with `new_file(path)` and transferred, which is the default. A file already present on one worker is declared `pre_staged(new_file(path))`, and the master neither reads nor sends it. A mount visible to every worker is not a task-level decision at all: declare its root once with `run_workflow(shared_path = ...)` and every absolute path under it is read in place. See sections 3.2.3 and 3.5.

## 11. Quick FAQ for chatbots

Q: Do I need a `wildcard_list` if my tasks do not use wildcards? A: No. `wildcard_list = NULL` (default) is valid for a workflow with fixed task instances.

Q: My task has only an `output`, no `input`. Is that valid? A: Yes. Only one of the two must be non-empty (`task.R`).

Q: Can a workflow have a single task? A: Yes. Single-task workflows pass DAG validation, since no connectivity check is needed. An unwrapped `task_object` is not accepted by `run_workflow()`: wrap it, `new_workflow(workflow_id = "wf", task_list = list(my_task))`.

Q: Where do logs go? A: Local subprocess logs go to the `log_dir` argument of `run_workflow()`, by default `log/` under the working directory, whose resolved path is printed at start. User code calling `to_log()` writes to the same directory, as `<sender>_log.log`. The stdout and stderr of an SSH-launched remote worker go to `/tmp/flumen_worker_<name>.log` on the remote host.

Q: How do I pass a Python interpreter that is not `python3` on PATH? A: `new_worker(python_cmd = "/path/to/python")`. Per-worker, not per-task. It governs Python tasks only: a Python based tool called from inside a shell body, such as `macs2`, is not affected by it (section 4.1.4).

Q: My shell task fails with status 127 and "command not found", but the tool is installed on that machine. A: The worker did not inherit the `PATH` that has it. A remote worker is started over a non interactive SSH session, which reads no login profile, so a conda activation or a `.bash_profile` entry is absent. There is no worker level environment setting: export the directory as the first line of the script body, or pass the binary's full path as an input parameter. See section 4.1.4.

Q: The same shell task succeeds on some runs and fails with 127 on others. A: The tool is installed at a different path on different nodes, and the outcome follows placement. The script text is built once and runs unchanged wherever the scheduler sends it. Either make the installation uniform across the eligible nodes, or pin the task with `requires = list(tags = ...)` matched by the worker's `resources`, which converts an intermittent runtime failure into a placement constraint. See section 4.1.4.

Q: Can I use `requires = list(cpu = 4)` on a local worker? A: Yes, by declaring the matching `resources` on the worker itself: `new_worker("w1", n_concurrent_task = 1, resources = list(cpu = 4))`, or with a capability label, `resources = list(tags = "mac")` matched by `requires = list(tags = "mac")`. This is the documented way to pin a task to the local machine, and it is the only placement control available for local workers. A worker that declares no `resources` accepts only tasks with `requires = NULL`.

Q: My workflow does not progress past startup. What do I check? A: The two usual causes are a master that cannot reach the worker, typically a firewall, and a worker that cannot reach the master, typically NAT. Inspect the log directory printed at startup. For SSH workers, run the `ssh ... 'Rscript -e "library(flumen)"'` command suggested in the error message to isolate the cause.

## 12. Source code quick map

| Section | Topic | File |
|------------------|---------------------------|---------------------------|
| 1.1 | Fundamental rule, `{x}` vs `{{x}}` | `R/wildcard_engine.R` |
| 3.1 | Wildcards | `R/wildcard_dsl.R`, `R/object_func.R`, `R/wildcard_engine.R` |
| 3.2 | Tasks | `R/task.R` |
| 3.2.1 | Script / Cmd | `R/task.R` |
| 3.2.2 | Input / Output | `R/fs.R` |
| 3.2.3 | File / Object / Dep | `R/fs.R` |
| 3.3 | Workflow | `R/workflow.R`, `R/workflow_prepare.R` |
| 3.4 | Worker | `R/worker.R` |
| 3.4.2 | Launchers | `R/launchers.R` |
| 3.5 | Execution | `R/execution.R` |
| 4.1.2 | Return values, outputs and multi-output | `inst/scripts/wrapper.R`, `R/wrapper_lib.R` |
| 4.1.3 | Outputs are never parameters | `R/par.R` |
| 4.2 | R task | `inst/scripts/wrapper.R` |
| 4.3 | Python task | `inst/scripts/wrapper.py`, `inst/scripts/objects.py` |
| 4.3.1 | Named multi-output IPC parity | `R/messaging.R`, `R/serialize.R` |
| 4.4 | Shell task | `inst/scripts/wrapper_shell.R` |
| 5.1 | Wrapping, rejected and correct forms | `R/validation.R` |
| 5.3.3 | Memory bounds of aggregation | `R/memory_cache.R`, `R/supervisor_deps.R` |
| 6.1 | Launcher constructors | `R/launcher_ssh.R` |
| 6.2 | Ports and reachability | `R/reachability.R` |
| 6.4 | Manual startup of a worker | `R/remote_worker.R` |
| 9.1 | `exe_id` after expansion | `R/task.R`, `R/utils.R` |
| 9.2 | Utilities | `R/preview.R`, `R/methods.R`, `R/status.R`, `R/utils.R`, `R/scheduler_task_table.R` |
| 9.3 | Fault detection, the thresholds | `R/scheduler_handlers.R`, `R/notify.R`, `R/diagnostics.R`, `inst/scripts/supervisor.R` |
