# A multi-language scRNA-seq pipeline with Flumen

This guide shows a scRNA-seq workflow that spans shell, R, and Python
across three machines (two Linux workstations and a Mac) with no shared
filesystem. The biology is cell-type annotation of whole blood from six
patients with mild COVID-19 (Bost *et al.* 2021, 10x 3' v3.1, GEO
accession `GSE157344`). It assumes the concepts from *Getting started*:
the difference between `{x}` and `{{x}}`, `new_dep`, and the `_exp` and
`_aggr` family for scatter and aggregate.

# The pipeline

The pipeline is made of four tasks. `count` runs one instance per sample
and produces a gene-expression matrix. `preprocess` also runs once per
sample, and its N Seurat objects are aggregated by `integrate` into a
single joint step. `integrate` emits two file outputs, which
`annotate` consumes as two separate dependencies on the same producer.

    count[shell]  ->  preprocess[R]  ->  integrate[R]  ->  annotate[Python]
    6 instances       6 instances        aggregate         aggregate

Three different transports appear in one run. The Cell Ranger matrices
move disk-to-disk, or are read in place when producer and consumer share
a worker. The per-sample Seurat objects cross in memory, serialized
with qs2, because a Seurat object is not one of the types Arrow can
carry. The two `integrate` outputs cross disk-to-disk to the Mac.

# 1. Wildcard definition

One wildcard, six values. Every task that mentions `{sample}` expands
into six instances.

```r
library(flumen)

w_list <- new_wildcard_list(
  sample = c("Blood32", "Blood33", "Blood34", "Blood35", "Blood36", "Blood37")
)
```

# 2. `count`: Cell Ranger

A shell task, one instance per sample. The reference transcriptome and
the FASTQ directories are `pre_staged()`: they are already on the
workers and Flumen must not try to move them.

Only the filtered matrix is declared as an output. Cell Ranger also
writes a BAM, but nothing downstream consumes it, and an output with no
consumer is wasted work in the delivery path.

`{{m}}` is dynamic: the number of files Cell Ranger writes into
`filtered_feature_bc_matrix/` is not known when the workflow is built,
so the pattern is matched against what the task actually produced.

```r
op_count <- new_shell_task(
  task_id = "count",
  input = new_input(
    sample        = new_object_exp("{sample}"),
    transcriptome = pre_staged(new_file("refdata-gex-GRCh38-2020-A")),
    fastqs        = pre_staged(new_file_exp("fastq/{sample}"))
  ),
  requires = list(tags = "linux"),
  output = new_output(
    matrix = new_file("count_{sample}/outs/filtered_feature_bc_matrix/{{m}}",
                      auto_mkdir = FALSE)
  ),
  exec = new_script(r"(
    #!/bin/bash
    cellranger count --id=count_{sample} --transcriptome={transcriptome} \
      --fastqs={fastqs} --sample={sample} \
      --localcores=8 --localmem=56 --nosecondary
)")
)
```

`auto_mkdir = FALSE` because Cell Ranger creates its own `--id`
directory and refuses to run if it already exists.

# 3. `preprocess`: Seurat QC

An R task, one instance per sample. The dependency uses
`new_file_aggr(..., by = "sample")`: a *partial* aggregation, so each
instance receives the files of its own sample only, as a list, rather
than all six samples' files.

The task returns a Seurat object, declared as `new_object("{{seu}}")`.
It is never written to disk: Flumen keeps it in the producing worker's
memory and sends it to the consumer that asks for it.

```r
op_preprocess <- new_R_task(
  task_id = "preprocess",
  input = new_input(
    sample_id = new_object_exp("{sample}"),
    mat_files = new_dep(
      task_id = "count",
      value   = new_file_aggr("count_{sample}/outs/filtered_feature_bc_matrix/{{m}}",
                              by = "sample")
    )
  ),
  requires = list(tags = "linux"),
  output = new_output(seu = new_object("{{seu}}")),
  exec = new_script('
    library(Seurat)
    preprocess_sample <- function(sample_id, mat_files) {
      sid <- sample_id

      bc <- grep("filtered_feature_bc_matrix/barcodes", mat_files, value = TRUE)
      if (length(bc) == 0L)
        stop("[preprocess] no filtered_feature_bc_matrix for sample ", sid,
             " (cwd=", getwd(), ")")
      mat_dir <- dirname(bc[1])

      counts <- Read10X(data.dir = mat_dir)
      seu <- CreateSeuratObject(counts = counts, project = sid,
                                min.cells = 3, min.features = 0)

      seu[["percent.mt"]] <- PercentageFeatureSet(seu, pattern = "^MT-")

      seu <- subset(seu, subset = nCount_RNA >= 500 & percent.mt <= 20)
      seu$sample <- sid
      seu <- NormalizeData(seu, verbose = FALSE)
      seu <- FindVariableFeatures(seu, selection.method = "vst",
                                  nfeatures = 2000, verbose = FALSE)
      flumen::to_log(sprintf("sample %s: %d cells after QC", sid, ncol(seu)),
                     sender = "preprocess")
      seu
    }
  ')
)
```

The body ends with the function, whose **value is the function itself**.
Flumen calls that value, so a trailing statement after the definition
would silently become the task's body and the function would be lost.

# 4. `integrate`: Seurat CCA, two file outputs

An R task with a full aggregation: `new_object_aggr("{{seu}}")` gathers
all six Seurat objects into one list for a single joint step. Two of
them arrive from the other Linux worker, in memory.

This task declares **two** outputs, and both must be files. A sparse
matrix is not one of the types Arrow can encode, and CellTypist needs
every gene, so the dense form would be several gigabytes. Since a task's
outputs must be all files or all objects, the embedding, which a
`data.frame` would keep in memory, follows the matrix to disk.

The two patterns live in **different directories**. `annotate_input/{{e}}`
is dynamic and claims everything in its directory, so a fourth file
written there would be collected along with the 10x triplet.

```r
op_integrate <- new_R_task(
  task_id = "integrate",
  input = new_input(
    seu_list = new_dep(task_id = "preprocess", value = new_object_aggr("{{seu}}"))
  ),
  requires = list(tags = "linux"),
  output = new_output(
    expr = new_file("annotate_input/{{e}}"),
    emb  = new_file("annotate_emb/embedding.tsv")
  ),
  exec = new_script('
    library(Seurat)
    library(Matrix)
    integrate_cca <- function(seu_list) {
      features <- SelectIntegrationFeatures(object.list = seu_list, nfeatures = 2000)
      anchors  <- FindIntegrationAnchors(object.list     = seu_list,
                                         anchor.features = features,
                                         reduction       = "cca",
                                         dims            = 1:30)
      integrated <- IntegrateData(anchorset = anchors, dims = 1:30)
      DefaultAssay(integrated) <- "integrated"
      integrated <- ScaleData(integrated, verbose = FALSE)
      integrated <- RunPCA(integrated, npcs = 30, verbose = FALSE)
      integrated <- RunUMAP(integrated, reduction = "pca", dims = 1:30, verbose = FALSE)
      integrated <- FindNeighbors(integrated, reduction = "pca", dims = 1:30, verbose = FALSE)
      integrated <- FindClusters(integrated, resolution = 0.5, verbose = FALSE)

      umap <- Embeddings(integrated, "umap")

      emb <- data.frame(
        cell    = colnames(integrated),
        sample  = as.character(integrated$sample),
        barcode = sub("-.*$", "", colnames(integrated)),
        UMAP_1  = umap[, 1],
        UMAP_2  = umap[, 2],
        cluster = as.character(Idents(integrated)),
        stringsAsFactors = FALSE
      )

      DefaultAssay(integrated) <- "RNA"
      integrated[["RNA"]] <- JoinLayers(integrated[["RNA"]])
      d <- LayerData(integrated, assay = "RNA", layer = "data")
      if (!setequal(colnames(d), colnames(integrated)))
        stop("[integrate] the joined RNA layer holds ", ncol(d),
             " cells and the object ", ncol(integrated))
      d <- d[, colnames(integrated), drop = FALSE]

      dir.create("annotate_input", showWarnings = FALSE, recursive = TRUE)
      writeMM(d, "annotate_input/matrix.mtx")
      if (system2("gzip", c("-f", "annotate_input/matrix.mtx")) != 0L)
        stop("[integrate] gzip of the expression matrix failed")
      writeLines(paste(rownames(d), rownames(d), "Gene Expression", sep = "\t"),
                 gzfile("annotate_input/features.tsv.gz"))
      writeLines(colnames(d), gzfile("annotate_input/barcodes.tsv.gz"))

      dir.create("annotate_emb", showWarnings = FALSE, recursive = TRUE)
      write.table(emb, "annotate_emb/embedding.tsv", sep = "\t",
                  quote = FALSE, row.names = FALSE, col.names = TRUE)

      invisible(NULL)
    }
  ')
)
```

# 5. `annotate`: CellTypist

A Python task, pinned to the Mac. It declares **two dependencies on the
same producer**, one per `integrate` output: the 10x triplet as an
aggregation, and the embedding as a single file.

Its one output is a table, not a figure. Colour, legend language and
point size are presentation choices that change without a single cell
changing, and each such edit would otherwise cost a run of this
pipeline. The csv carries the UMAP coordinates beside the labels, so it
stands alone and a figure can be drawn directly from it.

```r
op_annotate <- new_python_task(
  task_id = "annotate",
  input = new_input(
    expr      = new_dep(task_id = "integrate",
                        value = new_file_aggr("annotate_input/{{e}}")),
    embedding = new_dep(task_id = "integrate",
                        value = new_file("annotate_emb/embedding.tsv"))
  ),
  output = new_output(labels = new_file("results/celltypist_labels.csv")),
  requires = list(tags = "mac"),
  exec = new_script('
    import scanpy as sc
    import numpy as np
    import pandas as pd
    import celltypist
    import os

    GROUPS = {
      "Neutrophils":  ["Neutrophil","Activated Granulocyte"],
      "T cells":      ["CD4 T","CD4m T","CD4n T","CD8eff T","CD8m T","gd T"],
      "NK cells":     ["NK"],
      "Monocytes":    ["CD14 Monocyte","CD16 Monocyte"],
      "B cells":      ["B","Class-switched B","IgA PB","IgG PB"],
      "DC":           ["DC","pDC"],
      "Platelets":    ["Platelet"],
      "Erythrocytes": ["RBC"],
      "Eos / SC":     ["SC & Eosinophil"],
    }

    def annotate_blood(expr, embedding):
        mtx_dir = os.path.dirname(sorted(expr)[0])
        A = sc.read_10x_mtx(mtx_dir, var_names="gene_symbols", cache=False)
        A.var_names_make_unique()

        emb_path = embedding[0] if isinstance(embedding, (list, tuple)) else embedding
        emb = pd.read_csv(emb_path, sep="\t",
                          dtype={"cell": str, "sample": str,
                                 "barcode": str, "cluster": str})

        if list(A.obs_names) != list(emb["cell"].astype(str)):
            raise ValueError(
                "[annotate] matrix barcodes and embedding rows do not match: "
                "%d vs %d cells" % (A.n_obs, len(emb)))

        pred = celltypist.annotate(A, model="Adult_COVID19_PBMC.pkl",
                                   majority_voting=True)
        fine = pred.predicted_labels["majority_voting"].astype(str).values

        lookup = {v: k for k, vs in GROUPS.items() for v in vs}
        grp = np.array([lookup.get(x, "Other") for x in fine])

        os.makedirs("results", exist_ok=True)
        pd.DataFrame({
            "cell":       A.obs_names,
            "sample":     emb["sample"].astype(str).values,
            "cluster":    emb["cluster"].astype(str).values,
            "UMAP_1":     emb["UMAP_1"].values,
            "UMAP_2":     emb["UMAP_2"].values,
            "celltypist": fine,
            "lineage":    grp,
        }).to_csv("results/celltypist_labels.csv", index=False)
  ')
)
```

# 6. Assemble the workflow

`new_workflow()` validates the graph before any task runs: broken
dependencies, inconsistent wildcards and mixed output kinds become
build-time errors rather than mid-run failures.

```r
workflow <- new_workflow(
  workflow_id   = "scrnaseq_blood_annotation",
  desc          = "Whole blood: Cell Ranger -> Seurat CCA -> CellTypist",
  task_list     = list(op_count, op_preprocess, op_integrate, op_annotate),
  wildcard_list = w_list
)
```

# 7. Workers

Three machines, no shared filesystem. Cell Ranger ships only as a Linux
x86-64 binary, so `count`, `preprocess` and `integrate` carry
`requires = list(tags = "linux")` and `annotate` carries
`tags = "mac"`. A task runs only on a worker that advertises every tag
it requires.

`n_concurrent_task` is a plain slot count, not a capacity model: with
`--localmem=56` per Cell Ranger, the 64 GB machine receives one slot and the
128 GB machine two.

```r
worker_list <- list(
  new_worker(
    name              = "mac",
    n_concurrent_task = 1,
    python_cmd        = "/path/to/env/bin/python",
    resources         = list(tags = "mac")
  ),
  new_worker(
    name              = "remote1",
    self_host         = "10.0.0.1",
    root_path         = "/data/flumen",
    n_concurrent_task = 1,
    launcher          = ssh_launcher(
      host_user = "user@10.0.0.1",
      resources = list(cpu = 8, mem = "64G", tags = "linux")
    )
  ),
  new_worker(
    name              = "remote2",
    self_host         = "10.0.0.2",
    root_path         = "/data/flumen",
    n_concurrent_task = 2,
    launcher          = ssh_launcher(
      host_user = "user@10.0.0.2",
      resources = list(cpu = 8, mem = "128G", tags = "linux")
    )
  )
)
```

# 8. Pipeline execution

`master_host` is the address the workers dial back, so it must be
reachable from them.

```r
run_workflow(
  workflow,
  worker_list = worker_list,
  master_host = "10.0.0.100",
  chunk_size  = 1e8
)
```

While a long run proceeds, `flumen_status()` prints one row per task
instance with its worker and state, which is convenient for checking
progress without reading the logs.

```r
flumen_status()
```

A task that declares an output refuses to run if that output already
exists, so a second run in the same directory stops at the first
producer rather than mixing new results with old. Remove the previous
outputs, or run elsewhere.

# What this example demonstrates

-   **Three languages in one pipeline.** The tasks run in shell (Cell
    Ranger), then R (Seurat), then Python (CellTypist).
-   **No centralized storage is required.** There is no shared
    filesystem. Large inputs are `pre_staged()` on the worker that needs
    them, and every output moves directly from the producing worker to
    the consuming one.
-   **Objects and files, one mechanism.** The per-sample Seurat objects
    cross between machines **in memory**; the `integrate` outputs cross
    as files. Both are declared the same way and moved by the same
    machinery, differing only in substrate.
-   **Scatter and aggregate over samples.** `count` and `preprocess`
    expand into one instance per `{sample}`, while `integrate` and
    `annotate` aggregate. The `by = "sample"` partial aggregation keeps
    each `preprocess` reading only its own matrix.
-   **Several outputs from one task.** `integrate` declares a dynamic
    file bundle and a static file, and `annotate` depends on each
    separately, so the two arrive as two distinct inputs.

# Where to go next

-   [Getting started](getting-started.md), the building blocks, the
    `{x}` / `{{x}}` rules, and resource pinning (`requires` /
    `resources`).
-   [`FLUMEN_DSL.md`](FLUMEN_DSL.md), the exhaustive reference.
-   `?new_shell_task`, `?new_R_task`, `?new_python_task`, `?new_dep`,
    `?new_file_aggr`, `?pre_staged`, `?ssh_launcher`, `?run_workflow`.
