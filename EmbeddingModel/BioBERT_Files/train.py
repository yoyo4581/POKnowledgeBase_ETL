"""
Turns a TrainingData (see dataset.py) into a fine-tuned BioBERT retrieval
model, or exports/imports it as plain files for training elsewhere.

This machine may not have the resources (or even the packages -- torch,
transformers, sentence-transformers, datasets are not installed here as of
writing) to fine-tune BioBERT locally. The usual split across two machines:

  local (has Neo4j + SQL, no GPU):
    python -m EmbeddingModel.BioBERT_Files.train --export-only
    # -> writes data/qdrant/go_contrastive/ (train.jsonl + eval files)
    # upload that folder to the Colab session (Drive, or a direct upload)

  Colab (has a GPU, can't reach either database):
    python -m EmbeddingModel.BioBERT_Files.train --from-export data/qdrant/go_contrastive

--from-export skips extract()/build_dataset() entirely, so it never imports
Neo4jCaller or SQLCaller -- both would fail to import on a machine without
Neo4j configured / a SQL Server ODBC driver installed, which Colab is.

Run with no flags to build fresh from the live DBs and train locally,
falling back to export_dataset() automatically if the training stack isn't
importable.
"""
from __future__ import annotations

import json
from pathlib import Path

from EmbeddingModel.BioBERT_Files.dataset import Config, TrainingData, build_dataset, extract


def export_dataset(data: TrainingData, out_dir: str | Path = "data/qdrant/go_contrastive") -> Path:
    """
    Writes everything train_and_evaluate() needs as plain files, so a
    dataset built here (against this machine's live Neo4j + SQL) can be
    fine-tuned elsewhere, e.g. uploaded to a Google Colab notebook that
    mirrors train_and_evaluate()'s training loop.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(out_dir / "train.jsonl", "w") as f:
        for row in data.rows:
            f.write(json.dumps({k: row[k] for k in ("anchor", "positive", "negative")}) + "\n")

    (out_dir / "eval_queries.json").write_text(json.dumps(data.eval_queries, indent=2))
    (out_dir / "eval_corpus.json").write_text(json.dumps(data.eval_corpus, indent=2))
    (out_dir / "eval_relevant.json").write_text(
        json.dumps({k: sorted(v) for k, v in data.eval_relevant.items()}, indent=2)
    )
    (out_dir / "stats.json").write_text(json.dumps(data.stats, indent=2))

    print(f"Exported {len(data.rows)} training triplet(s) and the held-out eval set to {out_dir}")
    return out_dir


def import_dataset(in_dir: str | Path = "data/qdrant/go_contrastive") -> TrainingData:
    """
    Inverse of export_dataset(): rebuilds a TrainingData from files an
    earlier export_dataset() call wrote, so a dataset built once against
    this machine's live Neo4j + SQL can be trained on a machine that can't
    reach either (e.g. Colab) without rebuilding it there.
    """
    in_dir = Path(in_dir)

    rows = []
    with open(in_dir / "train.jsonl") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))

    eval_relevant = {
        term: set(genes)
        for term, genes in json.loads((in_dir / "eval_relevant.json").read_text()).items()
    }

    data = TrainingData(
        rows=rows,
        eval_queries=json.loads((in_dir / "eval_queries.json").read_text()),
        eval_corpus=json.loads((in_dir / "eval_corpus.json").read_text()),
        eval_relevant=eval_relevant,
        stats=json.loads((in_dir / "stats.json").read_text()),
    )
    print(f"Loaded {len(data.rows)} training triplet(s) and the held-out eval set from {in_dir}")
    return data


def plot_training_history(trainer, out_dir: str | Path,
                          metrics: tuple[str, ...] = ("accuracy@1", "accuracy@10", "ndcg@10", "mrr@10")) -> Path:
    """
    Plots training loss and a few held-out retrieval metrics from the
    Trainer's own log_history against epoch, side by side. Shows inline
    automatically in a notebook (Colab), and is saved to disk either way so
    it's available from a plain script run too.
    """
    import matplotlib.pyplot as plt

    history = trainer.state.log_history
    train_points = [(h["epoch"], h["loss"]) for h in history if "loss" in h]
    # Eval rows carry keys like "eval_heldout_go_cosine_accuracy@1" -- match
    # on the metric's suffix rather than hardcoding the evaluator's name
    # ("heldout_go") so this still works if that name ever changes.
    metric_keys = sorted({
        k for h in history for k in h
        if "cosine_" in k and any(k.endswith(f"_{m}") for m in metrics)
    })
    eval_points = {k: [(h["epoch"], h[k]) for h in history if k in h] for k in metric_keys}

    fig, (loss_ax, metric_ax) = plt.subplots(1, 2, figsize=(12, 4.5))

    if train_points:
        xs, ys = zip(*train_points)
        loss_ax.plot(xs, ys, marker="o")
    loss_ax.set_title("Training loss")
    loss_ax.set_xlabel("epoch")
    loss_ax.set_ylabel("loss")

    for k, points in eval_points.items():
        if not points:
            continue
        xs, ys = zip(*points)
        metric_ax.plot(xs, ys, marker="o", label=k.split("cosine_", 1)[-1])
    metric_ax.set_title("Held-out retrieval metrics")
    metric_ax.set_xlabel("epoch")
    metric_ax.set_ylabel("score")
    metric_ax.set_ylim(0, 1)
    metric_ax.legend(fontsize=8)

    fig.tight_layout()
    out_path = Path(out_dir) / "training_curves.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150)
    print(f"Saved training curves to {out_path}")
    plt.show()
    return out_path


def train_and_evaluate(
    data: TrainingData,
    base: str = "dmis-lab/biobert-base-cased-v1.2",
    out_dir: str = "biobert-go-retrieval",
    checkpoint_dir: str | None = None,
    batch_size: int = 256,
    epochs: int = 10,
    mini_batch_num_tokens: int = 32768,
    learning_rate: float = 2e-5,
    warmup_ratio: float = 0.1,
    use_cached_negatives: bool = True,
    gradient_checkpointing: bool = False,
    max_seq_length: int = 512,
    logging_steps: int = 20,
    metric_for_best_model: str = "heldout_go_cosine_ndcg@10",
    plot: bool = True,
):
    """
    Negatives: MultipleNegativesRankingLoss treats every OTHER positive in
    the same batch as an implicit extra negative for a given anchor, so
    effective negatives-per-anchor scales with batch_size, not with
    pos_per_anchor in the dataset -- a bigger batch is "more negatives" for
    free. Plain MultipleNegativesRankingLoss holds the whole batch's
    forward+backward pass in memory at once, so batch_size is capped
    directly by VRAM. CachedMultipleNegativesRankingLoss (default here,
    GradCache) still computes the loss over the full batch -- same
    effective negative count -- but processes the memory-heavy step in
    chunks, decoupling negatives (batch_size) from peak VRAM.

    That chunk budget is mini_batch_num_tokens, not a count of examples.
    With unpad_inputs=True below there is no padding, so a fixed example
    count would produce chunks of wildly different real size -- function
    texts run from one sentence to a dozen -- and leave the GPU underfed on
    the short ones. A token budget makes every chunk the same actual work.
    GradCache is exact at any chunk size (the gradients do not depend on
    it), so this is purely a speed/VRAM dial: raise it until you OOM.

    What does NOT make training faster is raising batch_size. The same
    tokens pass through the model either way, only grouped differently.
    What it changes is the optimizer-step count -- fewer, so at a fixed
    learning_rate you undertrain, see below -- and the in-batch negative
    pool. mini_batch_num_tokens is the speed knob; batch_size is the
    negatives knob.

    learning_rate does NOT auto-scale with batch_size: a bigger batch means
    fewer optimizer steps per epoch (same rows, fewer updates), so raising
    batch_size without also raising learning_rate under-trains relative to
    a smaller-batch run at the same epoch count -- roughly scale it with
    batch_size (the "linear scaling rule") when you push batch_size up.

    epochs defaults high (10) because overfitting is now handled by
    load_best_model_at_end below instead of by guessing the right epoch
    count: training loss can keep dropping past the point the held-out
    eval metrics peak (seen directly in an earlier run here -- eval scores
    peaked at epoch 8 of 10 and epochs 9-10 were strictly worse on every
    metric despite a lower training loss), so the checkpoint saved at
    trainer.train()'s end is not necessarily the last epoch, it's whichever
    epoch scored best on metric_for_best_model.
    """
    # Paths for sentence-transformers >= 6 / transformers >= 5
    # pip install "sentence-transformers[train]"   (pulls in datasets + accelerate)
    from datasets import Dataset
    from sentence_transformers import (SentenceTransformer, SentenceTransformerTrainer,
                                       SentenceTransformerTrainingArguments)
    from sentence_transformers.base.sampler import BatchSamplers
    from sentence_transformers.sentence_transformer import losses
    from sentence_transformers.sentence_transformer.evaluation import InformationRetrievalEvaluator
    from sentence_transformers.sentence_transformer.modules import Pooling, Transformer

    # bf16 + flash-attn2 + unpadding. All three need Ampere or newer (compute
    # capability >= 8.0) -- on a T4 this raises rather than silently falling back,
    # which is the behaviour we want: a slow run that looks fine is worse.
    #
    # max_seq_length is 512 rather than 256 because with unpad_inputs there is no
    # padding cost to a longer ceiling -- it only decides where truncation starts,
    # and the corpus has records well past 256 tokens.
    #
    # Loading in bfloat16 (torch_dtype) on top of bf16=True below means the
    # optimizer holds bf16 states rather than fp32 master weights. That is what
    # the previous model was trained with and it converged; if a future run looks
    # unstable early, dropping model_kwargs["torch_dtype"] while keeping bf16=True
    # is the first thing to try.
    word = Transformer(
        base,
        max_seq_length=max_seq_length,
        model_kwargs={"attn_implementation": "kernels-community/flash-attn2",
                      "torch_dtype": "bfloat16"},
        config_kwargs={"attention_probs_dropout_prob": 0.0},
        unpad_inputs=True,
    )
    pool = Pooling(word.get_embedding_dimension(), pooling_mode="mean")
    model = SentenceTransformer(modules=[word, pool])

    evaluator = InformationRetrievalEvaluator(
        queries=data.eval_queries, corpus=data.eval_corpus,
        relevant_docs=data.eval_relevant, name="heldout_go")
    print("raw BioBERT:", evaluator(model))           # the baseline to beat

    train_ds = Dataset.from_list([{k: r[k] for k in ("anchor", "positive", "negative")}
                                  for r in data.rows])
    loss = (losses.CachedMultipleNegativesRankingLoss(
                model, mini_batch_num_tokens=mini_batch_num_tokens)
            if use_cached_negatives else losses.MultipleNegativesRankingLoss(model))
    # Checkpoints go somewhere other than out_dir on purpose. They are whole
    # model copies (~440MB each, save_total_limit=2), and out_dir is what gets
    # zipped and shipped to the ETL host -- keeping them apart means the artifact
    # is just the model.
    checkpoint_dir = checkpoint_dir or f"{out_dir}-ckpt"
    args = SentenceTransformerTrainingArguments(
        output_dir=checkpoint_dir, num_train_epochs=epochs,
        per_device_train_batch_size=batch_size, learning_rate=learning_rate,
        warmup_steps=warmup_ratio, bf16=True,        # float in [0,1) = ratio; warmup_ratio was removed in transformers 5
        gradient_checkpointing=gradient_checkpointing,
        batch_sampler=BatchSamplers.NO_DUPLICATES,   # same text can't be pos and neg in one batch
        eval_strategy="epoch", logging_steps=logging_steps,
        # save_strategy must match eval_strategy for load_best_model_at_end
        # to work at all -- HF picks the best checkpoint from whichever
        # epochs actually got saved, so these can't disagree.
        save_strategy="epoch", save_total_limit=2,
        load_best_model_at_end=True, metric_for_best_model=metric_for_best_model, greater_is_better=True)
    trainer = SentenceTransformerTrainer(model=model, args=args, train_dataset=train_ds,
                                         loss=loss, evaluator=evaluator)

    import torch
    if torch.cuda.is_available():
        torch.cuda.reset_peak_memory_stats()  # isolate training's peak from the raw-BioBERT eval above

    trainer.train()
    print(f"Best checkpoint: {trainer.state.best_model_checkpoint} "
          f"(by {metric_for_best_model}={trainer.state.best_metric})")

    if torch.cuda.is_available():
        peak_gb = torch.cuda.max_memory_allocated() / 1e9
        print(f"Peak GPU memory during training: {peak_gb:.2f} GB "
              f"(batch_size={batch_size}, mini_batch_num_tokens={mini_batch_num_tokens})")

    print("fine-tuned:", evaluator(model))
    model.save(out_dir)

    if plot:
        # A missing matplotlib must not read as "training failed" -- the
        # model above is already trained and saved by this point, and
        # __main__ catches ImportError from this function to mean exactly
        # that (falling back to export_dataset()), which would otherwise
        # spuriously re-export the dataset on top of a perfectly good run.
        try:
            plot_training_history(trainer, out_dir)
        except ImportError as e:
            print(f"Skipping training curves ({e}); the model itself was trained and saved fine.")

    return trainer


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--from-export", metavar="DIR", nargs="?", const="data/qdrant/go_contrastive", default=None,
        help="Train on a dataset an earlier --export-only run wrote (default: data/qdrant/go_contrastive), "
             "instead of building fresh from Neo4j + SQL. This is the mode to run on Colab.",
    )
    parser.add_argument(
        "--export-only", action="store_true",
        help="Build the dataset from Neo4j + SQL and write it to disk instead of training.",
    )
    parser.add_argument("--epochs", type=int, default=10,
                        help="Training epochs (default: 10). Safe to run high -- load_best_model_at_end means "
                             "the checkpoint that gets saved is whichever epoch scored best, not the last one, "
                             "so extra epochs at worst cost time, not quality.")
    parser.add_argument("--metric-for-best-model", default="heldout_go_cosine_ndcg@10",
                        help="Eval metric used to pick which epoch's checkpoint gets saved (default: "
                             "heldout_go_cosine_ndcg@10). Must match a key InformationRetrievalEvaluator logs, "
                             "without the 'eval_' prefix -- e.g. heldout_go_cosine_mrr@10, "
                             "heldout_go_cosine_map@100, heldout_go_cosine_accuracy@1.")
    parser.add_argument("--batch-size", type=int, default=256,
                        help="Effective batch size = in-batch negatives per anchor (default: 256). "
                             "Raising it does NOT speed training up -- it changes negatives and step count, "
                             "not throughput. See train_and_evaluate's docstring.")
    parser.add_argument("--mini-batch-num-tokens", type=int, default=32768,
                        help="Token budget per GradCache chunk -- the actual VRAM and SPEED knob "
                             "(default: 32768). GradCache is exact at any chunk size, so raise this "
                             "until you OOM; lower it if you already do. A token budget rather than an "
                             "example count because unpad_inputs means examples have no fixed cost.")
    parser.add_argument("--max-seq-length", type=int, default=512,
                        help="Truncation ceiling (default: 512). Free to raise with unpad_inputs -- "
                             "there is no padding, so short texts cost nothing extra.")
    parser.add_argument("--checkpoint-dir", default=None,
                        help="Where HF writes per-epoch checkpoints (default: <out-dir>-ckpt). Kept out "
                             "of --out-dir so the shipped artifact is only the model, not 2x440MB of "
                             "checkpoints. On Colab point this at /content/ckpt.")
    parser.add_argument("--learning-rate", type=float, default=2e-5,
                        help="Default: 2e-5. Does NOT auto-scale with --batch-size -- raising batch_size without "
                             "raising this under-trains (fewer optimizer steps/epoch at the same data size); "
                             "roughly scale it with batch_size (the linear scaling rule) when you push batch up.")
    parser.add_argument("--warmup-ratio", type=float, default=0.1,
                        help="Fraction of training spent ramping the learning rate up from 0 (default: 0.1).")
    parser.add_argument("--no-cached-negatives", action="store_true",
                        help="Use plain MultipleNegativesRankingLoss instead of the GradCache variant "
                             "(couples batch_size to VRAM directly again -- mainly for comparison/debugging).")
    parser.add_argument("--gradient-checkpointing", action="store_true",
                        help="Trade compute for activation memory. Try this before shrinking "
                             "--mini-batch-num-tokens further if you're still OOMing.")
    parser.add_argument("--out-dir", default="biobert-go-retrieval",
                        help="Where to save the model + training_curves.png (default: biobert-go-retrieval). "
                             "Give each trial its own dir so runs don't overwrite each other's model/plot.")
    parser.add_argument("--logging-steps", type=int, default=20,
                        help="How often (in optimizer steps) to log training loss for the plot (default: 20). "
                             "A bigger --batch-size means fewer steps/epoch, so lower this to keep the loss "
                             "curve from being too sparse to read.")
    parser.add_argument("--no-plot", action="store_true",
                        help="Skip plotting/saving training_curves.png (e.g. on a machine without matplotlib).")
    args = parser.parse_args()

    if args.from_export and args.export_only:
        parser.error("--from-export already skips building from the databases; --export-only has nothing to build.")

    if args.from_export:
        data = import_dataset(args.from_export)
    else:
        from src.builders.Neo4j.Neo4jCaller import Neo4j_ETL
        from src.builders.SQL.SQLCaller import SQL_ETL

        neo4j_caller = Neo4j_ETL()
        sql_caller = SQL_ETL()

        raw = extract(neo4j_caller, sql_caller)
        data = build_dataset(**raw, cfg=Config())

    print(data.stats)

    if args.export_only:
        export_dataset(data)
    else:
        # The export fallback exists for the ETL host, which has no GPU stack:
        # build the dataset and hand it off rather than dying. It used to wrap
        # train_and_evaluate() in `except ImportError`, which was far too wide --
        # ANY ImportError from inside training (a version-pinned kernels package,
        # a missing CUDA extension) came back as "exporting instead" and exit 0.
        # A crashed fine-tune reported as a successful export is how you end up
        # building a Qdrant collection against a model that was never written.
        # So the check happens up front, and anything that fails during training
        # now raises.
        try:
            import datasets            # noqa: F401
            import sentence_transformers  # noqa: F401
            import torch               # noqa: F401
        except ImportError as e:
            print(f"Training stack not importable ({e}).")
            if args.from_export:
                raise SystemExit(
                    "Refusing to re-export a dataset that was just imported. "
                    "Install the training stack, or run this where it exists.")
            print("Exporting the dataset for external training instead.")
            export_dataset(data)
        else:
            train_and_evaluate(
                data,
                out_dir=args.out_dir,
                checkpoint_dir=args.checkpoint_dir,
                batch_size=args.batch_size,
                epochs=args.epochs,
                mini_batch_num_tokens=args.mini_batch_num_tokens,
                max_seq_length=args.max_seq_length,
                learning_rate=args.learning_rate,
                warmup_ratio=args.warmup_ratio,
                use_cached_negatives=not args.no_cached_negatives,
                gradient_checkpointing=args.gradient_checkpointing,
                logging_steps=args.logging_steps,
                metric_for_best_model=args.metric_for_best_model,
                plot=not args.no_plot,
            )
