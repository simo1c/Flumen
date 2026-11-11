# Getting Started with Flumen

This is a practical guide for writing and running `flumen` workflows. It
covers the building blocks and the main rules. For a full worked
pipeline, see [A multi-language scRNA-seq pipeline with
Flumen](scrnaseq-annotation.md). For a complete reference, see
[`FLUMEN_DSL.md`](FLUMEN_DSL.md), which is also an LLM-friendly resource
for automatic workflow construction.

# 1. What is Flumen?

Flumen is a bioinformatics workflow manager for multi-language
pipelines, embedded in the R session. It runs R, Python and shell tasks
across one or more machines moving in-memory objects and files directly
between tasks with no centralized storage or external orchestrator.

In practice, `flumen` schedules the tasks, each an R, Python, or shell
computation, across one or more worker processes. Tasks declare their
inputs and outputs, and `flumen` derives the dependency graph (directed
acyclic graph, DAG), places tasks on workers, and moves data between
them over IPC (local workers) or TCP/IP (remote). Both in-memory objects
and files are moved using nanomsg-next-gen (NNG). An object is
serialized with Apache Arrow IPC when its type is Arrow-compatible (data
frames, matrices, vectors), which lets it move to another language.
R-only types (lists, fitted models, ...) are serialized with the `qs2`
package and can be exchanged between R tasks. Files are sent unchanged
in their original binary format. There is no disk round-trip, and no
shared filesystem is required.

The DAG is parametrised by wildcards and expanded both at build and run
time. A single-sample pipeline grows to N samples by adding wildcard
values, with no change to the task definitions.

Flumen is useful when:

-   You have workflows that mix R, Python and a shell task and you do not
    want to use a centralized storage.
-   You run the same pipeline over many inputs with the expansion
    handled for you.
-   You move expensive steps to a larger machine or a cluster by changing
    only the worker list, with no pipeline rewrite.

## Installation

```r
# install.packages("remotes")
remotes::install_github("simo1c/Flumen")
```

`flumen` is written in R but contains a small amount of C++, so
installing from source needs a compiler.

R dependencies install automatically from CRAN (`nanonext`, `arrow`,
`qs2`, `igraph`, `sys`, `uuid`, `rlang`, `Rcpp`). For Python tasks,
every worker that runs them needs an interpreter with:

```bash
pip3 install pyarrow pandas numpy pynng
```

All dependencies are imported when the Python wrapper starts, so a
missing one stops the task before your code runs.

# 2. The Flumen building blocks

A `flumen` pipeline is assembled from four types of object:

1.  **Wildcards** (`new_wildcard`, `new_wildcard_list`) are named sets
    of values that parametrise the pipeline, such as
    `sample = c("s1","s2","s3")`, or
    `dataset = list(d1 = data.frame(...), d2 = data.frame(...))`. Values
    can be plain strings or full R objects.
2.  **Tasks** (`new_R_task`, `new_python_task`, `new_shell_task`) are
    computation tasks, with declared inputs (`new_input`), outputs
    (`new_output`), code (`new_script` or `new_cmd`), and optional
    resource requirements (`requires`).
3.  **Workflow** (`new_workflow`) is a set of tasks and a wildcard list.
    The constructor validates the workflow features (unique ids, valid
    `exec`, dependency references resolve, the DAG is connected and
    acyclic) and expands the wildcards into concrete task instances.
4.  **Workers** (`new_worker`, optionally with `ssh_launcher`) are the
    processes that run tasks. They can be local or remote.

The workflow is executed with
`run_workflow(workflow, worker_list, chunk_size, ...)`.

A small end-to-end workflow:

```r
library(flumen)

w_list <- new_wildcard_list(sample = c("s1", "s2", "s3"))

A <- new_R_task(
  task_id = "A",
  input   = new_input(x = new_object_exp("{sample}")),
  output  = new_output(out = new_object("{{out}}")),
  exec    = new_script('
    f <- function(x) paste("processed", x)
  ')
)

B <- new_R_task(
  task_id = "B",
  input   = new_input(xs = new_dep(task_id = "A", value = new_object_aggr("{{out}}"))),
  output  = new_output(final = new_object("{{final}}")),
  exec    = new_script('
    function(xs){
      g <- paste(unlist(xs), collapse = "; ")
      saveRDS(g, "test.Rds")
    }
  ')
)

workflow <- new_workflow(
  workflow_id   = "workflow",
  desc          = "Expand A over {sample}, aggregate in B",
  task_list     = list(A, B),
  wildcard_list = w_list
)

run_workflow(
  workflow,
  worker_list = list(new_worker(name = "w1", n_concurrent_task = 1))
)
```

The example above covers wildcard expansion, task definition,
aggregation, dependencies, and a local worker. The rest of the guide
explains each building block.

`run_workflow()` only requires the workflow and the worker list.
Everything else has a default, including `chunk_size`, the largest
amount of data sent in one message, which is 100 MB.

Degenerate cases are all valid: no wildcards (`wildcard_list = NULL`,
default), output-only tasks (no `input`), and single-task workflows.

# 3. Static and dynamic wildcards

Flumen uses two wildcard forms in paths and values. Static `{x}` is
substituted at build time from `new_wildcard_list`, while dynamic
`{{x}}` is resolved at run time from a task's actual outputs.

| Notation | Meaning | Resolution | Where to use it |
|------------------|------------------|------------------|------------------|
| `{name}` | a static wildcard that substitutes each value from `new_wildcard_list` | build time | argument of `new_file*` / `new_object*` |
| `{{name}}` | a dynamic output, a key the running task produces | at run time | argument of `new_file*` / `new_object*` |
| `{name}` inside a `new_script()` body (shell) | a parameter placeholder that substitutes the `new_input` / `new_output` value | by the shell wrapper at run time | shell scripts only |

Examples of correct use of wildcards:

```r
new_object_exp("{sample}")               # bind this input to each value of the `sample` wildcard
new_file_exp("fastq/{sample}.fastq.gz")  # one task instance per sample; that sample's fastq
new_object("{{out}}")                    # a runtime-produced in-memory object called "out"
new_file("bam/{sample}.{{lane}}.bam")    # static {sample} and dynamic {{lane}} in one path, allowed
```

# 4. Declaring inputs, outputs and dependencies

## `new_input` / `new_output`

`new_input(...)` and `new_output(...)` take named arguments. For an R or
Python task they become the function's argument names (inputs) and the
keys you return (outputs). For a shell task they become the `{name}`
placeholders in the script. For shell tasks, *unnamed* inputs are
allowed (passed positionally), and names can be CLI flags
(`"--threads" = 8`, `"-o" = ...`).

```r
new_input(threads = 8, reads = new_file("reads.fq"))
#  R/Python: the function must be  function(threads, reads) { ... }
#  shell:    {threads} and {reads} get substituted in the script
```

A task needs at least one of `input` / `output` non-empty.

## Objects vs files

| Constructor | Object | Type of transfer | Usage |
|------------------|------------------|------------------|------------------|
| `new_object(...)` | an in-memory R/Python value | passed between tasks via Arrow IPC / qs2 | data frames, matrices, lists, fitted models, ... |
| `new_file(...)` | a path on disk | the bytes are transferred (or pre-staged, see section 7) | any data a tool reads or writes as a file: fastq, bam, gtf, rds, ... |

`new_object()` with no argument auto-generates a UUID placeholder, a
concise way to declare an output you do not need to name:
`new_output(out = new_object())`.

## `new_dep`: linking a consumer to a producer

A task consumes another task's output by wrapping the producer's output
pattern in `new_dep(task_id = "<producer>", value = <pattern>)`:

```r
producer <- new_R_task(
  task_id = "A",
  input   = new_input(df = new_object_exp("{dataset}")),
  output  = new_output(out = new_object("{{out_A}}")),
  exec    = new_script('f <- function(df) { df$z <- df$x + 1; df }')
)

consumer <- new_R_task(
  task_id = "B",
  input   = new_input(df = new_dep(task_id = "A", value = new_object_exp("{{out_A}}"))),
  output  = new_output(out = new_object("{{out_B}}")),
  exec    = new_script('g <- function(df) summary(df)')
)
```

`value` must match one of the producer's `new_output(...)` patterns. A
typo (or renaming the producer's output) is caught when you build the
workflow. You can pass several patterns to one `new_dep(...)` to depend
on more than one of the same parent's outputs.

## Scatter and aggregate: the `_exp` / `_aggr` family

This is how a pipeline written for one item runs over N, and how the N
results come back together.

| Variant | Effect | Notes |
|------------------------|:----------------------:|------------------------|
| `new_object("...")` / `new_file("...")` | 1-to-1 | one upstream value becomes one downstream value |
| `new_object_exp("{wc}")` / `new_file_exp("dir/{wc}.ext")` | **scatter** | one task instance per wildcard value |
| `new_object_aggr("{{name}}")` / `new_file_aggr("dir/{wc}.ext")` | **aggregate** | all upstream instances become one list for a single consumer |
| `new_object_aggr("{{name}}", by = "wc1")` / `new_file_aggr("dir/{wc1}_{wc2}.ext", by = "wc1")` | **partial aggregate** | one consumer per `wc1`, each getting a list aggregated over the *other* wildcards |

`_exp` and `_aggr` are forbidden inside `new_output(...)`, because
expansion of a task follows from its *inputs*, not its outputs.

A minimal scatter and aggregate example:

```r
w_list <- new_wildcard_list(sample = c("s1", "s2", "s3", "s4"))

per_sample <- new_R_task(           # runs 4 times, once per sample
  task_id = "stat",
  input   = new_input(s = new_object_exp("{sample}")),
  output  = new_output(out = new_object("{{stat}}")),
  exec    = new_script('f <- function(s) data.frame(sample = s, n = nchar(s))')
)

combine <- new_R_task(              # runs once, gets a list of all 4 results
  task_id = "combine",
  input   = new_input(parts = new_dep(task_id = "stat", value = new_object_aggr("{{stat}}"))),
  output  = new_output(out = new_object("{{table}}")),
  exec    = new_script(
    'function(parts){
      parts <- do.call(rbind, parts)
      saveRDS(parts, "test.Rds")
    } 
    '
  )
)

workflow <- new_workflow(
  workflow_id   = "scatter_aggregate",
  desc          = "per-sample stat, then combine",
  task_list     = list(per_sample, combine),
  wildcard_list = w_list
)

worker_list <- list(
  new_worker(name = "worker", n_concurrent_task = 1)
)

run_workflow(workflow, worker_list)
```

## Multi-file outputs and reference directories

There is no `new_folder()` constructor. A task that writes a directory
of files is handled with `new_file()` / `new_file_aggr()`.

-   A tool that writes a deterministic family of files (for example
    `bowtie2-build`, which writes `idx.1.bt2`, `idx.2.bt2`, ...) needs
    the static part backtick-quoted and a numeric dynamic wildcard, as
    in `` new_file('`index/idx`.{{N}}.bt2') ``, and the consumer
    aggregates them with `` new_file_aggr('`index/idx`.{{N}}.bt2') ``.
-   A tool that writes files with runtime-determined names (e.g.
    `fastqc`, `cellranger`) needs the directory prefix backtick-quoted
    and a free dynamic wildcard, as in
    `` new_file('`outdir/`{{any}}.txt') ``.
-   A pre-existing reference directory (a genome index, a database) is
    declared as an input only,
    `pre_staged(new_file("/refs/star_index"))`, and the tool consumes it
    in place.
-   An explicit bundle of files whose names are known at build time,
    `new_file(c("a.fa", "b.fa", "c.fa"))`, makes each entry a transfer
    slot, and the consumer receives a vector (or list) of resolved
    paths.

The backticks in the first two bullets mark the *static* part of a
pattern and are **shell-only**: they are rejected at build time in a
`new_R_task()` or `new_python_task()` parameter. The tools in those
examples (`bowtie2-build`, `fastqc`, `cellranger`) are external
commands, so they are shell tasks and the backtick form is the correct one
there.

Note: `new_output(new_file("output_dir"))`, a single path segment with
no extension, no wildcard, no backtick (or a path ending in `/`), is
rejected at build time as an "ambiguous output path". Use the backtick +
dynamic form, or add an extension/wildcard.

For shell consumers of a backtick-quoted prefix, the substituted
`{name}` resolves to the static prefix with no trailing slash, so append
the separator yourself: `{ref}/genome` not `{ref}genome`.

# 5. Writing tasks

## R tasks

The parameter `exec` is a `new_script('...')` whose body is parsed and
evaluated. The value of the last top-level expression must be a
function, so in practice, end the script with a function definition.
`flumen` calls that function. Helper functions defined earlier in the
script are allowed, and only the last one is the entry point.

```r
A <- new_R_task(
  task_id = "A",
  input   = new_input(x = 1, y = "abc"),
  output  = new_output(out = new_object("{{out}}")),
  exec    = new_script('
    helper  <- function(z) z * 2
    my_func <- function(x, y) list(result = paste(helper(x), y))
  ')
)
```

-   The function's arguments must match the `new_input` names, and
    *only* those. `new_output` names are stripped before the call, so
    the function never receives them.

What to return depends on the output declaration:

| `new_output(...)` declaration | Return | Notes |
|------------------------|------------------------|------------------------|
| one plain output: `out = new_object("{{out}}")` | any single value (df, scalar, vector, list, model, ...) | auto-wrapped, with type detected for Arrow vs qs2 |
| one plain output: `out = new_file("path.txt")` | ignored, just write the file in the body | output files are detected by snapshot + glob, not by the return value |
| a multi-instance output: `out = new_object("{{out}}")` consumed downstream as scatter | `list(v1, v2, ..., vN)`, where `length()` becomes the number of potential downstream instances | one consumer instance per element if it expands that input |
| a runtime-named file output: `new_file("dir/{{any}}.txt")` | ignored, just write files matching the pattern | the wrapper globs them afterwards. The backtick-quoted form of this pattern is shell-only and is rejected here |
| multiple outputs | a named list: `list(out1 = v1, out2 = v2, ...)` | the names survive IPC and reach the consumer |

```r
exec = new_script('f <- function(x) x + 1')                     # single output, scalar
exec = new_script('f <- function(df) list(df, df, df)')         # 3 downstream instances
exec = new_script('f <- function(x) list(a = x*2, b = x*3)')    # named multi-output {{a}}, {{b}}
```

## Python tasks

The parameter `exec` is a `new_script('...')` defining a function. For
several named outputs it returns a `dict` mapping output names to
values; for a single output it may return the value directly (a
`DataFrame`, array, list or scalar), auto-wrapped like an R single-value
return. Data moves between R and Python through Arrow IPC. A
`pandas.DataFrame` produced by Python arrives as an R `data.frame` in
the next task with no intermediate files. Python is indent-sensitive,
but `flumen` auto-dedents the string, so you can indent freely inside
the R quotes.

```r
B <- new_python_task(
  task_id = "B",
  input   = new_input(df = new_dep(task_id = "A", value = new_object_exp("{{out_A}}"))),
  output  = new_output(out = new_object("{{out_B}}")),
  exec    = new_script('
    def summarise(df):
        return {"out": df.__class__({
            "n_rows": [len(df)],
            "mean_x": [float(df["x"].mean())],
        })}
  ')
)
```

The worker's interpreter needs `pyarrow`, `pandas`, `numpy` and `pynng`
(all four are imported at wrapper start-up, so a Python task fails
without them). The `python3` on path can be specified in each worker:
`new_worker(python_cmd = "/usr/local/bin/python3.11")`.

## Shell/bin tasks

There are two ways to declare a command.

**`new_cmd("toolname")`** is preferred for a single tool with no shell
logic. `flumen` builds the argument list directly from `new_input` /
`new_output` (in declaration order: inputs, then outputs) and executes
the binary directly, with no shell involved, so no quoting, globbing, or
escaping pitfalls. Named entries become flags (`"-w" = 4` becomes
`-w 4`), unnamed entries pass positionally, and a simple flag is just an
unnamed string.

```r
trim <- new_shell_task(
  task_id = "trim",
  input   = new_input("-w" = 4, "-i" = new_file_exp("fastq/{sample}.fastq.gz")),
  output  = new_output("-o" = new_file("trim/{sample}.fastq.gz"),
                       "-h" = new_file("reports/{sample}.html"),
                       "-j" = new_file("reports/{sample}.json")),
  exec    = new_cmd("fastp")
)
```

**`new_script('...')`** is useful when you actually need shell features:
pipes, redirections, derived variables, multiple commands, conditionals.
`{name}` placeholders in the body are replaced with the matching
parameter value (file paths are substituted as plain strings).

```r
align <- new_shell_task(
  task_id = "align",
  input   = new_input(threads = 8,
                      index   = "/refs/genome",   # an index read by prefix is a plain string
                      reads   = new_file_exp("trim/{sample}.fastq.gz")),
  output  = new_output(bam = new_file("bam/{sample}.bam")),
  exec    = new_script('
    bowtie2 -p {threads} -x {index} -U {reads} | samtools sort -@ {threads} -o {bam} -
  ')
)
```

Points to note:

-   **The backslash-newline pitfall.** If you split a shell command
    across several lines and end a line with a backslash to continue it,
    as you normally would in a shell script, R silently deletes that
    backslash inside a single-quoted string. Your single command then
    breaks into two, and the shell task fails with status 127 (command
    not found). Three ways to avoid it: (1) put each command on one
    line, with no trailing backslash; (2) write the script as an R raw
    string, `r"( ... )"`, which keeps the backslash; or (3) preferably,
    use `new_cmd()` when the task is just a single tool call.

-   **Shell inputs must be simple.** A shell task can consume atomic
    scalars and vectors, logicals, file paths, and lists of those, but
    not a `data.frame`, matrix, array, or list of complex objects (it
    would error at run time with "cannot consume parameter '...' as
    data.frame"). If an upstream R/Python task produces a complex
    value, have it write a file (`.tsv`, `.rds`, `.parquet`) and let
    the shell task consume the path.

# 6. Running and inspecting

```r
run_workflow(
  workflow,
  worker_list = list(new_worker(name = "w1", n_concurrent_task = 2)),
  chunk_size  = 1e8        # max bytes per network chunk; 1e8 (100 MB)
)
```

-   `chunk_size` bounds a single message chunk, so large payloads are
    split into chunks of at most this size.
-   `n_concurrent_task` indicates how many tasks can be executed
    concurrently in that worker.
-   `run_workflow()` blocks until the workflow finishes. It accepts an object of class
    `workflow_object`. To launch without blocking the R session, run the
    script as a background OS process (e.g.
    `nohup Rscript pipeline.R &`).

Before running, you can inspect the workflow in different ways:

```r
preview_workflow(workflow)         # static plan: tasks, instances, edges, port budget
print(workflow)                    # one-line summary: how many tasks and instances
plot(workflow)                     # render the DAG (uses igraph)
print_workflow_commands(workflow)  # per task: the resolved command + its bound parameters
length(workflow$task_list)         # how many instances after wildcard expansion
```

`preview_workflow()` is the most informative of these, and it runs
nothing: use it to check that the wildcards expanded into the instances
you expected before committing to a long run.

## Getting the results back

`run_workflow()` returns `NULL`. Results are not brought back into the
calling session: an object output stays in the worker cache, which is
what lets the next task read it without a disk round-trip. To keep
a result after the run, declare a file output and read it afterwards:

```r
B <- new_R_task(
  task_id = "B",
  input   = new_input(xs = new_dep(task_id = "A", value = new_object_aggr("{{out}}"))),
  output  = new_output(res = new_file("result.txt")),
  exec    = new_script('
    g <- function(xs) {
      writeLines(paste(unlist(xs), collapse = "; "), "result.txt")
      invisible(NULL)
    }
  ')
)

workflow <- new_workflow(        # rebuild it: the workflow captured the previous B
  workflow_id   = "workflow",
  desc          = "Expand A over {sample}, aggregate in B",
  task_list     = list(A, B),
  wildcard_list = w_list
)

run_workflow(workflow, worker_list = list(new_worker(name = "w1", n_concurrent_task = 2)))
readLines("result.txt")            # the file is on disk once the run finishes
```

The same applies to any format: write an `.rds`, a `.csv` or a figure
from the task, declare it with `new_file()`, and read it back from the
session.

**Logging.** A fresh log directory is created per run, and its path is
printed at start (override with `run_workflow(log_dir = ...)`). The
scheduler, each supervisor and each task wrapper write their own file
there, so you can follow them while debugging a long run. From inside a
task, append your own lines with `to_log()`:

```r
new_script('
  f <- function(df) {
    flumen::to_log(paste("rows:", nrow(df)), sender = "myop")
    df
  }
')
```

# 7. Running across machines

A worker is local (scheduler-spawned, IPC) or remote (another machine,
which dials back over TCP/IP). The `workflow` object is
deployment-agnostic. The same object runs on a laptop or across a
cluster by changing only `worker_list`.

## What makes a worker remote

A worker is remote when it has a launcher. With `launcher = NULL` (the
default) the worker is local: the scheduler spawns it on the master's
own machine over IPC, and no TCP port is used for it.

## SSH auto-launch

`ssh_launcher()` opens an SSH connection, starts
`flumen::start_worker()` on the remote host, and the worker dials back.
Before launching, `flumen` pre-flights the remote working directory (see
the error in section 9), so a missing directory or an unreachable host
is reported clearly rather than as a generic startup timeout.

```r
w_remote <- new_worker(
  name              = "remote1",
  self_host         = "192.168.1.50",      # the remote's IP/hostname, as seen by the master and by peers
  root_path         = "/scratch/work",     # working dir on the remote (and the launcher's default cwd)
  n_concurrent_task = 4,
  launcher          = ssh_launcher(host_user = "myuser@192.168.1.50", host_port = 22L)
)
```

## Starting a remote worker manually

When the master cannot reach the other machine over SSH, you can
still use it, by starting the worker yourself and letting it dial the
master.

A launcher is any object with a `launch_worker()` method, so a launcher
that does nothing gives you exactly that: `flumen` reserves the TCP
ports and waits for the worker, without attempting to start it.

```r
manual_launcher <- function(resources = NULL) {
  structure(list(resources = resources), class = "manual_launcher")
}
launch_worker.manual_launcher <- function(config, worker) invisible(TRUE)

w_remote <- new_worker(
  name              = "remote1",
  self_host         = "192.168.1.50",   # as seen by the master and by peers
  n_concurrent_task = 4,
  launcher          = manual_launcher(resources = list(cpu = 8, mem = "64G"))
)
```

Then start the worker on the other machine, in an R session whose working
directory is the worker's `root_path` (`ssh_launcher()` arranges this with
`cd`; a session started elsewhere places files inconsistently). Build
`master_url` from the port plan `run_workflow()` prints at launch, using
the notify port and that worker's control port:

```r
setwd("/data/flumen_run")   # the worker's root_path
flumen::start_worker(master_url = "tcp://192.168.1.10:61000,61001", worker_id = "remote1")
```

`worker_id` must equal the `name` of the matching `new_worker(...)`, and
the master waits for it up to `run_workflow(startup_timeout = ...)`.
Declare `resources` on the launcher, not on the worker, exactly as for
`ssh_launcher()`.

You can also pass a plain `function(worker)` as the launcher for one-off
custom bootstrap, although a plain function carries no `resources`.

## Ports: two different pools

There are two separate, easy-to-confuse port ranges:

-   **`run_workflow(ports = 61000:61099)`** is the master's listen pool.
    `ports[1]` is a single result-uplink (notify) port shared by *all*
    remote workers; then each remote worker uses 2 consecutive ports
    (control + terminate). A workflow with `n` remote workers needs
    `length(ports) >= 1 + 2*n`. Local workers do not consume from it.
-   **`new_worker(ports = 61100:61199)`** is that worker's own
    contiguous TCP range for peer-to-peer transfer. `ports[1]` is its
    data-request listen socket (open for the entire run, dialed by the
    master and by peer workers), while `ports[2:N]` is a transfer pool
    that is cycled and reused (never grows past the range). It is a no-op
    for a local worker in an all-local workflow, where peer transfer is
    in-process.

Both must be contiguous integer ranges within [1024, 65535]. Two workers
on the same host must have disjoint `ports` (and disjoint from the
master's pool if the master is co-located), and `run_workflow()` errors
if they overlap. If a worker's transfer pool is too small for its
`n_concurrent_task`, the run aborts with a clear message.

At startup `run_workflow()` prints a port plan: one section per host
that has to accept inbound connections, giving the port range, the
addresses that dial it, and whether it carries control traffic or data
transfers, so you can pre-check the firewall. If a remote run
terminates at startup, the most likely
causes are (1) the master cannot reach a worker's data-request port
(firewall on the worker) or (2) the worker cannot reach the master (NAT /
incorrect `master_host`), and the master emits a diagnostic naming the port
it could not reach.

```r
w_local <- new_worker(name = "local1", n_concurrent_task = 2)

run_workflow(
  workflow,
  worker_list      = list(w_local, w_remote),
  chunk_size       = 1e8,
  master_host      = "192.168.1.10",   # the master's address, reachable by the remotes; auto-detected if NULL
  ports            = 61000:61099       # master's per-remote-worker listen pool (the default)
)
```

## Pre-staged data and shared storage

By default the master reads a `new_file()`'s bytes at dispatch and
transfers them to the worker. For data already on the worker, you have
to wrap the file input in `pre_staged()`. The master neither reads nor
transfers it, and the worker finds it locally. A mismatch (declared
pre-staged but missing on the worker) is reported as a structured error
before the task's wrapper starts. This is typical for genome indexes,
annotation files, large reference databases.

```r
new_input(gtf = pre_staged(new_file("/refs/gencode.v44.annotation.gtf")))
```

`pre_staged()` accepts only a `new_file()` or `new_file_exp()` value. It
marks one file as already present on a specific worker. A filesystem
visible to *every* worker is instead declared once at
launch with `run_workflow(shared_path = ...)`. Any file whose path lies
under a declared root is read by the consumer from that root, so flumen
transfers nothing and the task definitions do not change.

This applies to a task's **outputs** as well, not only to the inputs you
declare yourself. When a file output under a declared root is consumed by a
downstream task on another machine, the consumer reads it from the mount
instead of receiving the bytes peer-to-peer. So the same pipeline moves
between a set of machines with no shared storage, where outputs are transferred
over sockets, and a cluster with an NFS or Lustre mount, where they are not, by
declaring the mount at launch. An output declared with `new_object()` is
unaffected: an in-memory value is always transferred over the socket.

# 8. Resource pinning

Each task can declare what kind of worker it needs. The scheduler only
dispatches a task to a worker that satisfies it. If none can,
`run_workflow()` aborts at validation time.

| Entity | Code location | Argument |
|------------------------|------------------------|------------------------|
| **task**, what it needs | `new_R_task` / `new_python_task` / `new_shell_task` | `requires = list(cpu = , mem = , gpu = , tags = )` |
| **worker**, what it offers | `ssh_launcher` for a launched worker, `new_worker` for a local one | `resources = list(cpu = , mem = , gpu = , tags = )` |

Both use the same schema, a named list with optional `cpu` (non-negative
integer), `mem` (a string with a unit: `"32G"`, `"128GB"`, `"16384M"`,
`"1.5T"`, or an integer = bytes), `gpu` (non-negative integer), and
`tags` (a character vector of capability labels). Any other field name
is an error.

Requirements and resources are matched as follows:

-   For `cpu` and `gpu`, the worker must offer at least the requested
    amount.
-   For `mem`, both strings are parsed to bytes and compared (i.e.,
    `"128G"` = `"128GB"` = `"128000M"`).
-   For `tags`, the worker must advertise every requested tag (set
    membership, AND). Tags express capabilities the numeric fields
    cannot, such as `tags = "cellranger"` to keep a task on the machines
    where a tool is installed.
-   a field in `requires` that is absent from the worker's `resources`
    means no match (you cannot satisfy `gpu = 1` from a worker that
    does not mention GPUs).
-   a worker that declares no `resources` matches only tasks with
    `requires = NULL`. A local worker has no launcher to carry them, so it
    declares them on itself, `new_worker(resources = list(...))`; declaring
    them on both the worker and its launcher is an error.

Among the workers that satisfy a task, the locality-aware policy selects
one.

This is an example on how tasks with different requirements are assigned
to workers with different capabilities:

```r
gpu_worker  <- new_worker(
  name = "gpu1",  self_host = "10.0.0.21",
  launcher = ssh_launcher(host_user = "me@gpu-node",
                          resources = list(cpu = 16, mem = "128G", gpu = 1)))
cpu_worker  <- new_worker(
  name = "cpu1",  self_host = "10.0.0.22",
  launcher = ssh_launcher(host_user = "me@cpu-node",
                          resources = list(cpu = 64, mem = "256G")))
util_worker <- new_worker(
  name = "util1", self_host = "10.0.0.23",
  launcher = ssh_launcher(host_user = "me@util-node",
                          resources = list(cpu = 8, mem = "16G")))

train  <- new_R_task(
  task_id  = "train",
  input    = new_input(data = new_dep("prep", new_object("{{m}}"))),
  output   = new_output(model = new_object("{{model}}")),
  exec     = new_script('f <- function(data) fit_model(data)'),
  requires = list(cpu = 8,  mem = "32G", gpu = 1)     # GPU-bound
)
crunch <- new_R_task(
  task_id  = "crunch",
  input    = new_input(x = new_dep("train", new_object("{{model}}"))),
  output   = new_output(out = new_object("{{c}}")),
  exec     = new_script('f <- function(x) heavy_compute(x)'),
  requires = list(cpu = 32, mem = "64G")              # many cores, big RAM
)
report <- new_R_task(
  task_id  = "report",
  input    = new_input(x = new_dep("crunch", new_object("{{c}}"))),
  output   = new_output(out = new_object("{{r}}")),
  exec     = new_script('f <- function(x) summarise(x)'),
  requires = list(cpu = 4,  mem = "8G")               # light
)
```

Workers are selected as follows:

-   **`train`** (`gpu = 1`): only `gpu1` advertises a GPU, so every
    `train` instance runs there, while `cpu1` and `util1` have no `gpu`
    field and do not match.
-   **`crunch`** (`cpu = 32`): only `cpu1` (64 cores) qualifies, since
    `gpu1` (16) and `util1` (8) fail on CPU.
-   **`report`** (`cpu = 4, mem = "8G"`): every worker qualifies, and
    among them the locality-aware policy prefers the one already holding
    `crunch`'s output, which avoids a cross-worker transfer.

If you pass a pool with no GPU worker, the run stops before launching
any task:

```r
#> Flumen workflow rejected.
#>   call      : run_workflow()
#>   task      : train
#>   error     : requires resources that no worker in worker_list can satisfy:
#>               cpu=8, mem=32G, gpu=1
#>   fix       : add a worker that declares matching or greater resources, on
#>               new_worker() for a local worker or on its launcher for a remote
#>               one, or relax the requirement
```

For a homogeneous pool, leave `requires = NULL` everywhere and
`resources = NULL` on launchers, and the scheduler falls back to pure
locality-aware placement.

# 9. Common pitfalls: quick reference

| Symptom / message | Cause | Fix |
|------------------------|------------------------|------------------------|
| `'exec' must be created with new_script()` | simple string passed to `exec` | wrap it: `new_script('...')` |
| `the task declares neither input nor output` | both `input` and `output` are `NULL` | add at least one |
| `the DAG is disconnected` | the task graph is not connected: an isolated task, or two chains each complete but never referencing each other | link the parts with a `new_dep`, or run them as separate workflows. A single-task workflow is accepted |
| `dependency cycle in task_list, involving task_ids: ...` | a cycle, such as A depends on B depends on A | redesign the flow |
| `unknown field(s) '<name>', allowed: 'cpu', 'mem', 'gpu', 'tags'` | a field outside `{cpu, mem, gpu, tags}` | use only the allowed fields |
| `requires resources that no worker ... can satisfy` | the pool does not cover a task's `requires` | add a worker with matching `resources`, or relax `requires` |
| `'self_host' is reserved for remote workers` | `self_host = ...` on a local worker (`launcher = NULL`) | remove it (locals inherit `master_host` from `run_workflow`) |
| warning `does not exist on the master` | `new_file("literal/path")` is transferred by default but the file is missing on the master | stage it on the master, wrap it in `pre_staged()` if it is on the worker, or use a wildcard if the path is dynamic |
| dynamic output not populated | the function did not return a named list with the `{{name}}` key | return `list(<output_name> = ..., <dyn_key> = ...)` |
| `ambiguous output path '<p>', which looks like a folder` | `new_file("output_dir")`, a single segment with no extension/wildcard/backtick (also raised on a trailing `/`) | use `` new_file('`output_dir`/{{out}}') ``, or add an extension/wildcard |
| `new_dep(task_id = 'X', value = '<v>') matches no output of 'X'` | the `new_dep` `value` does not match any of the producer's `new_output` patterns (typo / producer renamed) | align the consumer pattern with one of the producer's outputs |
| shell task fails with `status 127`, every binary present | the `\<newline>` pitfall, where R single-quoted strings eat the backslash | one command per line, or R raw strings `r"(...)"`, or `new_cmd()` |
| `[shell op X] cannot consume parameter 'Y' as data.frame` (or matrix/array/raw/...) | a shell task received a complex object from an upstream R/Python task | have the upstream task write a file (`.tsv`/`.rds`/`.parquet`), and the shell task consumes the path |
| `'ports' must be a contiguous range of >= 2 values in [1024, 65535]` | a non-contiguous range, a value out of range, or fewer than 2 ports | pass e.g. `61100:61199` (worker) / `61000:61099` (master) |
| `'ports' has N ports but K remote worker(s) need M` | `run_workflow(ports=...)` too small: it needs 1 shared notify port + 2 (control + terminate) per remote worker, i.e. `1 + 2*K` | widen `run_workflow(ports = ...)` |
| `overlapping TCP port ranges: 'w1' A..B and 'w2' C..D` | two workers on one host (or the master + a co-located worker) overlap | shift one worker's `new_worker(ports = ...)` so the ranges are disjoint |
| `peer-to-peer port pool (...) exhausted` | a worker's transfer pool is too small for its concurrency | widen `new_worker(ports = ...)` or lower `n_concurrent_task` |
| `the remote working directory <dir> does not exist on <host>` (or `cannot reach the host over SSH`) | the SSH pre-flight check, where the launcher's `cwd` (defaulting to the worker's `root_path`) is missing on the remote (or SSH failed) | `ssh <host> mkdir -p <dir>`, or pass a valid `cwd`/`root_path`, and for SSH, check the passwordless key + reachability |

# Where to go next

-   `?new_workflow`, `?new_worker`, `?run_workflow` are the main entry
    points.
-   `?new_R_task`, `?new_python_task`, `?new_shell_task` are the three
    task types. `?new_input`, `?new_output`, `?new_dep`,
    `?new_object_exp`, `?new_object_aggr`, `?new_file_exp`,
    `?new_file_aggr` cover declaring data flow, while `?ssh_launcher`
    and `?start_worker` cover remote setup.
-   [`FLUMEN_DSL.md`](FLUMEN_DSL.md) is the exhaustive reference (every
    signature, every caveat, decision trees), kept in sync with the
    code.
