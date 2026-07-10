"""Model runner abstraction (Day 2).

Owns the model lifecycle (load once, reuse forever) and the batch tensor
shape work: assembling a batch of variable-length requests into padded,
masked tensors, and splitting the model's batched output back into
per-request results.

The model itself is deliberately tiny and defined in-repo (embedding ->
mask-aware mean pool -> linear). This project is about the serving system,
not the model; an in-repo model keeps tests deterministic, startup instant,
and the repo free of checkpoint downloads. What matters -- and what real
serving systems must also get right -- is that batching NEVER changes a
request's result. Padding one request to another request's length must not
leak into the math. That property is enforced by test.

Everything runs on CPU in inference mode.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import torch
from torch import nn


@dataclass(frozen=True)
class ModelConfig:
    """Hyperparameters for the in-repo model.

    Inputs are sequences of integer token ids in [0, vocab_size); each
    request produces a vector of n_outputs floats. Weights are generated
    from `seed`, so two runners with the same config are identical --
    which is what makes cross-process determinism testable.
    """

    vocab_size: int = 1024
    embed_dim: int = 32
    n_outputs: int = 8
    seed: int = 20260709


class _TinySequenceModel(nn.Module):
    """Embedding -> mask-aware mean pooling -> linear head.

    The mask (1.0 for real tokens, 0.0 for padding) does the heavy
    lifting: padded positions are zeroed before the sum, and the divisor
    is the count of REAL tokens per row, not the padded length. Together
    those two facts are why the pad token id is irrelevant and why
    batching cannot change a request's output.
    """

    def __init__(self, cfg: ModelConfig) -> None:
        super().__init__()
        self.embed = nn.Embedding(cfg.vocab_size, cfg.embed_dim)
        self.head = nn.Linear(cfg.embed_dim, cfg.n_outputs)

    def forward(self, tokens: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
        # tokens: [B, L_max] int64; mask: [B, L_max] float32
        emb = self.embed(tokens)                    # [B, L_max, D]
        emb = emb * mask.unsqueeze(-1)              # zero padded positions
        n_real = mask.sum(dim=1, keepdim=True)      # [B, 1] real-token counts
        pooled = emb.sum(dim=1) / n_real            # [B, D] mean over real tokens
        return self.head(pooled)                    # [B, n_outputs]


class ModelRunner:
    """Loads the model once and serves `predict(batch)` calls.

    Intended lifecycle: construct at process startup, call `load()` once,
    then call `predict()` from the scheduler for every batch. `predict()`
    is stateless with respect to previous calls.
    """

    PAD_ID = 0  # value used to fill padded positions; masked out, so any valid id works

    def __init__(self, config: ModelConfig | None = None) -> None:
        self.config = config or ModelConfig()
        self._model: _TinySequenceModel | None = None

    @property
    def is_loaded(self) -> bool:
        return self._model is not None

    def load(self) -> None:
        """Build the model with seeded weights and freeze it in eval mode.

        Idempotent: calling load() again on a loaded runner is a no-op,
        so accidental double-initialization at startup can't swap weights
        mid-flight.
        """
        if self._model is not None:
            return
        torch.manual_seed(self.config.seed)
        model = _TinySequenceModel(self.config)
        model.eval()
        self._model = model

    def predict(self, batch: Sequence[Sequence[int]]) -> list[list[float]]:
        """Run one batch of variable-length requests.

        Args:
            batch: one token-id sequence per request; lengths may differ.

        Returns:
            One list of `n_outputs` floats per request, in input order.
        """
        if self._model is None:
            raise RuntimeError("ModelRunner.predict() called before load()")
        self._validate(batch)
        tokens, mask = self._assemble(batch)
        with torch.inference_mode():
            out = self._model(tokens, mask)         # [B, n_outputs]
        return self._disassemble(out)

    # ---------------------------------------------------------------- #
    # Batch shape work: the actual ML-systems content of this module.  #
    # ---------------------------------------------------------------- #

    def _assemble(
        self, batch: Sequence[Sequence[int]]
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """Pad variable-length requests into rectangular tensors.

        Requests of different lengths can't be stacked directly, so every
        row is padded with PAD_ID up to the longest request IN THIS BATCH
        (not a global max -- no wasted compute on short batches), and a
        parallel {1.0, 0.0} mask records which positions are real.
        """
        lengths = [len(seq) for seq in batch]
        l_max = max(lengths)
        tokens = torch.full(
            (len(batch), l_max), self.PAD_ID, dtype=torch.int64
        )
        mask = torch.zeros((len(batch), l_max), dtype=torch.float32)
        for i, seq in enumerate(batch):
            tokens[i, : lengths[i]] = torch.as_tensor(seq, dtype=torch.int64)
            mask[i, : lengths[i]] = 1.0
        return tokens, mask

    @staticmethod
    def _disassemble(out: torch.Tensor) -> list[list[float]]:
        """Split the batched [B, n_outputs] output into per-request rows.

        Row i of the output belongs to request i of the input -- order is
        the contract. (Once the scheduler exists, this ordering is what
        routes each result back to the correct caller's future.)
        """
        return out.tolist()

    def _validate(self, batch: Sequence[Sequence[int]]) -> None:
        if len(batch) == 0:
            raise ValueError("batch must contain at least one request")
        vocab = self.config.vocab_size
        for i, seq in enumerate(batch):
            if len(seq) == 0:
                raise ValueError(f"request {i} is an empty sequence")
            for t in seq:
                if not isinstance(t, int) or isinstance(t, bool):
                    raise ValueError(
                        f"request {i} contains non-integer token {t!r}"
                    )
                if not 0 <= t < vocab:
                    raise ValueError(
                        f"request {i} token {t} out of range [0, {vocab})"
                    )
