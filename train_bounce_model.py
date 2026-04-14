from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from derive_quality_rules import ResolvedCase, build_cases, load_jsonl

try:
    import joblib
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.feature_extraction import DictVectorizer
    from sklearn.preprocessing import FunctionTransformer
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
except Exception as exc:  # noqa: BLE001
    raise SystemExit(
        "Missing ML deps. Install: pip install scikit-learn joblib\n"
        f"Import error: {exc}"
    )


NUM_FEATURES = [
    "wall_ratio",
    "wall_dominance_ratio",
    "wall_notional_usd",
    "wall_distance_from_spread_pct",
    "day_volume_usd",
    "visible_age_sec",
    "seen_count",
    "wall_notional_stability_ratio",
    "round_level_score",
    "wall_notional_volatility_ratio",
    "distance_volatility_ratio",
    "pre_touch_decay_ratio",
    "distance_compression_ratio",
    "rebuild_count",
    "touch_survival_sec",
    "touch_attempt_count",
    "updates_before_touch",
    "approach_speed_pct_per_sec",
]
CAT_FEATURES = ["market", "side", "liquidity_bucket"]


def case_to_row(case: ResolvedCase) -> dict[str, Any]:
    return {
        "market": case.market,
        "side": case.side,
        "wall_ratio": case.wall_ratio,
        "wall_dominance_ratio": case.wall_dominance_ratio,
        "wall_notional_usd": case.wall_notional_usd,
        "wall_distance_from_spread_pct": case.wall_distance_from_spread_pct,
        "day_volume_usd": case.day_volume_usd,
        "visible_age_sec": case.visible_age_sec,
        "seen_count": float(case.seen_count),
        "wall_notional_stability_ratio": case.wall_notional_stability_ratio,
        "round_level_score": case.round_level_score,
        "wall_notional_volatility_ratio": case.wall_notional_volatility_ratio,
        "distance_volatility_ratio": case.distance_volatility_ratio,
        "pre_touch_decay_ratio": case.pre_touch_decay_ratio,
        "distance_compression_ratio": case.distance_compression_ratio,
        "rebuild_count": float(case.rebuild_count),
        "touch_survival_sec": case.touch_survival_sec,
        "touch_attempt_count": float(case.touch_attempt_count),
        "updates_before_touch": float(case.updates_before_touch),
        "approach_speed_pct_per_sec": case.approach_speed_pct_per_sec,
        "liquidity_bucket": case.liquidity_bucket,
    }


def build_target(case: ResolvedCase, target_mode: str, favorable_min: float, loss_max: float) -> int:
    if target_mode == "bounce":
        return 1 if case.outcome == "bounced" else 0
    # tradable target: good reaction + controlled adverse move
    return 1 if (case.favorable_move_pct >= favorable_min and case.loss_proxy_pct <= loss_max) else 0


def expectancy_of_subset(cases: list[ResolvedCase]) -> float:
    if not cases:
        return 0.0
    wins = [c for c in cases if c.outcome == "bounced"]
    fails = [c for c in cases if c.outcome == "failed"]
    n = len(cases)
    wr = len(wins) / n
    lr = 1.0 - wr
    avg_win = sum(max(0.0, c.favorable_move_pct) for c in wins) / len(wins) if wins else 0.0
    avg_loss = sum(max(0.0, c.loss_proxy_pct) for c in fails) / len(fails) if fails else 0.0
    return (wr * avg_win) - (lr * avg_loss)


def precision_at_topk(y_true: list[int], proba: list[float], top_frac: float) -> tuple[float, list[int]]:
    n = len(y_true)
    if n == 0:
        return 0.0, []
    k = max(1, int(n * top_frac))
    ranked_idx = sorted(range(n), key=lambda i: proba[i], reverse=True)[:k]
    tp = sum(y_true[i] for i in ranked_idx)
    return (tp / k), ranked_idx


def split_time_sorted(cases: list[ResolvedCase]) -> tuple[list[ResolvedCase], list[ResolvedCase], list[ResolvedCase]]:
    ordered = sorted(cases, key=lambda c: c.touched_ts)
    n = len(ordered)
    i1 = max(1, int(n * 0.60))
    i2 = max(i1 + 1, int(n * 0.80))
    train = ordered[:i1]
    valid = ordered[i1:i2]
    test = ordered[i2:]
    return train, valid, test


def make_xy(cases: list[ResolvedCase], target_mode: str, favorable_min: float, loss_max: float) -> tuple[list[dict[str, Any]], list[int]]:
    x = [case_to_row(c) for c in cases]
    y = [build_target(c, target_mode, favorable_min, loss_max) for c in cases]
    return x, y


def evaluate_model(
    model: Any,
    x: list[dict[str, Any]],
    y: list[int],
    cases: list[ResolvedCase],
    top_frac: float = 0.10,
) -> dict[str, float]:
    if not x:
        return {
            "auc": 0.0,
            "precision_top": 0.0,
            "expectancy_top": 0.0,
            "count": 0.0,
        }
    proba = model.predict_proba(x)[:, 1].tolist()
    try:
        auc = float(roc_auc_score(y, proba)) if len(set(y)) > 1 else 0.5
    except Exception:
        auc = 0.0
    p_top, idx = precision_at_topk(y, proba, top_frac=top_frac)
    top_cases = [cases[i] for i in idx]
    exp_top = expectancy_of_subset(top_cases)
    return {
        "auc": auc,
        "precision_top": p_top,
        "expectancy_top": exp_top,
        "count": float(len(x)),
    }


def make_logreg_pipeline() -> Pipeline:
    return Pipeline(
        steps=[
            ("vec", DictVectorizer(sparse=True)),
            ("scale", StandardScaler(with_mean=False)),
            ("clf", LogisticRegression(max_iter=500, class_weight="balanced")),
        ]
    )


def make_hgb_pipeline() -> Pipeline:
    return Pipeline(
        steps=[
            ("vec", DictVectorizer(sparse=True)),
            (
                "to_dense",
                FunctionTransformer(
                    lambda x: x.toarray() if hasattr(x, "toarray") else x,
                    accept_sparse=True,
                ),
            ),
            ("clf", HistGradientBoostingClassifier(max_depth=6, learning_rate=0.06, max_iter=220)),
        ]
    )


def train_config(
    train_cases: list[ResolvedCase],
    valid_cases: list[ResolvedCase],
    test_cases: list[ResolvedCase],
    *,
    target_mode: str,
    favorable_min_pct: float,
    loss_max_pct: float,
    top_frac: float,
) -> dict[str, Any]:
    x_train, y_train = make_xy(train_cases, target_mode, favorable_min_pct, loss_max_pct)
    x_valid, y_valid = make_xy(valid_cases, target_mode, favorable_min_pct, loss_max_pct)
    x_test, y_test = make_xy(test_cases, target_mode, favorable_min_pct, loss_max_pct)

    models: dict[str, Any] = {
        "logreg": make_logreg_pipeline(),
        "hgb": make_hgb_pipeline(),
    }

    best_name = ""
    best_model = None
    best_expectancy = -1e9
    best_precision = -1e9
    results: dict[str, Any] = {}
    for name, model in models.items():
        try:
            model.fit(x_train, y_train)
            valid_metrics = evaluate_model(model, x_valid, y_valid, valid_cases, top_frac=top_frac)
            test_metrics = evaluate_model(model, x_test, y_test, test_cases, top_frac=top_frac)
        except Exception as exc:  # noqa: BLE001
            results[name] = {
                "error": str(exc),
                "valid": {
                    "auc": 0.0,
                    "precision_top": 0.0,
                    "expectancy_top": 0.0,
                    "count": float(len(x_valid)),
                },
                "test": {
                    "auc": 0.0,
                    "precision_top": 0.0,
                    "expectancy_top": 0.0,
                    "count": float(len(x_test)),
                },
            }
            continue

        results[name] = {
            "valid": valid_metrics,
            "test": test_metrics,
        }
        valid_exp = float(valid_metrics.get("expectancy_top", 0.0))
        valid_prec = float(valid_metrics.get("precision_top", 0.0))
        if (valid_exp > best_expectancy) or (
            abs(valid_exp - best_expectancy) <= 1e-9 and valid_prec > best_precision
        ):
            best_expectancy = valid_exp
            best_precision = valid_prec
            best_name = name
            best_model = model

    return {
        "best_model_name": best_name,
        "best_model": best_model,
        "results": results,
        "counts": {
            "train": len(train_cases),
            "valid": len(valid_cases),
            "test": len(test_cases),
        },
    }


def parse_float_grid(raw: str) -> list[float]:
    values: list[float] = []
    for part in raw.split(","):
        item = part.strip()
        if not item:
            continue
        values.append(float(item))
    if not values:
        raise ValueError("empty grid")
    return values


def main() -> None:
    parser = argparse.ArgumentParser(description="Train bounce ranking model with time-based split")
    parser.add_argument("--log-path", default="data/signal_events_all_pairs.jsonl")
    parser.add_argument("--out-model", default="data/models/bounce_model.joblib")
    parser.add_argument("--out-meta", default="data/models/feature_columns.json")
    parser.add_argument("--target-mode", choices=["bounce", "tradable"], default="bounce")
    parser.add_argument("--favorable-min-pct", type=float, default=0.35)
    parser.add_argument("--loss-max-pct", type=float, default=0.20)
    parser.add_argument("--top-frac", type=float, default=0.10)
    parser.add_argument("--tradable-search", action="store_true")
    parser.add_argument("--favorable-grid", default="0.20,0.35,0.50,0.75")
    parser.add_argument("--loss-grid", default="0.10,0.15,0.20,0.30")
    parser.add_argument("--search-out-json", default="data/reports/tradable_target_search.json")
    args = parser.parse_args()

    events = load_jsonl(Path(args.log_path))
    if not events:
        raise SystemExit("No events found.")
    all_cases = [c for c in build_cases(events) if c.outcome in {"bounced", "failed"}]
    if len(all_cases) < 300:
        raise SystemExit(f"Not enough resolved cases for ML: {len(all_cases)}")

    train_cases, valid_cases, test_cases = split_time_sorted(all_cases)
    if args.tradable_search:
        if args.target_mode != "tradable":
            raise SystemExit("--tradable-search requires --target-mode tradable")
        favorable_grid = parse_float_grid(args.favorable_grid)
        loss_grid = parse_float_grid(args.loss_grid)
        combos: list[dict[str, Any]] = []
        best_combo: dict[str, Any] | None = None
        for favorable_min in favorable_grid:
            for loss_max in loss_grid:
                trained = train_config(
                    train_cases,
                    valid_cases,
                    test_cases,
                    target_mode=args.target_mode,
                    favorable_min_pct=favorable_min,
                    loss_max_pct=loss_max,
                    top_frac=args.top_frac,
                )
                best_name = trained["best_model_name"]
                results = trained["results"]
                if not best_name:
                    summary = {
                        "favorable_min_pct": favorable_min,
                        "loss_max_pct": loss_max,
                        "best_model": "",
                        "valid_auc": 0.0,
                        "valid_precision_top": 0.0,
                        "valid_expectancy_top": 0.0,
                        "test_auc": 0.0,
                        "test_precision_top": 0.0,
                        "test_expectancy_top": 0.0,
                        "results": results,
                    }
                else:
                    valid = results[best_name]["valid"]
                    test = results[best_name]["test"]
                    summary = {
                        "favorable_min_pct": favorable_min,
                        "loss_max_pct": loss_max,
                        "best_model": best_name,
                        "valid_auc": float(valid["auc"]),
                        "valid_precision_top": float(valid["precision_top"]),
                        "valid_expectancy_top": float(valid["expectancy_top"]),
                        "test_auc": float(test["auc"]),
                        "test_precision_top": float(test["precision_top"]),
                        "test_expectancy_top": float(test["expectancy_top"]),
                        "results": results,
                    }
                combos.append(summary)
                if best_combo is None:
                    best_combo = summary
                else:
                    if (
                        summary["test_expectancy_top"] > best_combo["test_expectancy_top"]
                        or (
                            abs(summary["test_expectancy_top"] - best_combo["test_expectancy_top"]) <= 1e-9
                            and summary["test_precision_top"] > best_combo["test_precision_top"]
                        )
                    ):
                        best_combo = summary
                print(
                    f"combo favorable={favorable_min:.2f} loss={loss_max:.2f} "
                    f"best={summary['best_model'] or 'none'} "
                    f"valid_auc={summary['valid_auc']:.4f} "
                    f"valid_p@top={summary['valid_precision_top']:.4f} "
                    f"valid_exp@top={summary['valid_expectancy_top']:.4f}% | "
                    f"test_auc={summary['test_auc']:.4f} "
                    f"test_p@top={summary['test_precision_top']:.4f} "
                    f"test_exp@top={summary['test_expectancy_top']:.4f}%"
                )

        out_json = Path(args.search_out_json)
        out_json.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "target_mode": args.target_mode,
            "top_frac": args.top_frac,
            "split_counts": {
                "train": len(train_cases),
                "valid": len(valid_cases),
                "test": len(test_cases),
            },
            "favorable_grid": favorable_grid,
            "loss_grid": loss_grid,
            "combinations": combos,
            "best": best_combo,
        }
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        if best_combo is None:
            raise SystemExit("No combinations evaluated.")
        print(
            f"Selected tradable target: favorable={best_combo['favorable_min_pct']:.2f} "
            f"loss={best_combo['loss_max_pct']:.2f} model={best_combo['best_model'] or 'none'}"
        )
        print(f"Saved search report: {out_json}")
        return

    trained = train_config(
        train_cases,
        valid_cases,
        test_cases,
        target_mode=args.target_mode,
        favorable_min_pct=args.favorable_min_pct,
        loss_max_pct=args.loss_max_pct,
        top_frac=args.top_frac,
    )
    best_name = trained["best_model_name"]
    best_model = trained["best_model"]
    results = trained["results"]

    if best_model is None:
        raise SystemExit("Model training failed.")

    out_model = Path(args.out_model)
    out_meta = Path(args.out_meta)
    out_model.parent.mkdir(parents=True, exist_ok=True)
    out_meta.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(best_model, out_model)
    meta = {
        "target_mode": args.target_mode,
        "favorable_min_pct": args.favorable_min_pct,
        "loss_max_pct": args.loss_max_pct,
        "top_frac": args.top_frac,
        "num_features": NUM_FEATURES,
        "cat_features": CAT_FEATURES,
        "best_model": best_name,
        "results": results,
        "split_counts": {
            "train": len(train_cases),
            "valid": len(valid_cases),
            "test": len(test_cases),
        },
    }
    out_meta.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Cases resolved: {len(all_cases)}")
    print(f"Split: train={len(train_cases)} valid={len(valid_cases)} test={len(test_cases)}")
    for name in ("logreg", "hgb"):
        r = results[name]
        if r.get("error"):
            print(f"{name}: error={r['error']}")
            continue
        print(
            f"{name}: valid_auc={r['valid']['auc']:.4f} "
            f"valid_p@top={r['valid']['precision_top']:.4f} "
            f"valid_exp@top={r['valid']['expectancy_top']:.4f}% | "
            f"test_auc={r['test']['auc']:.4f} "
            f"test_p@top={r['test']['precision_top']:.4f} "
            f"test_exp@top={r['test']['expectancy_top']:.4f}%"
        )
    print(f"Selected model: {best_name}")
    print(f"Saved model: {out_model}")
    print(f"Saved metadata: {out_meta}")


if __name__ == "__main__":
    main()
