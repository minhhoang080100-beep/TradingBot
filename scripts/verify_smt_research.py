#!/usr/bin/env python3
"""Read-only independent audit of fixed SMT reports and original source candles."""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIRECTORIES = (ROOT / "paper-runs/research-20261029-30d",
               ROOT / "paper-runs/research-20261029-entry-v2-prior30d")
FIELDS = ("metrics", "final_snapshot", "trade_events", "daily_last_marks")
SEGMENTS = ("development", "holdout")
SMT_NAMES = tuple(f"smt_f{fractal}{m5}_{suffix}"
                  for m5 in ("", "_m5_aligned") for fractal in (3, 5)
                  for suffix in ("base", "stressed_costs", "delayed_base", "delayed_stressed_costs"))
MINUTE = timedelta(minutes=1)


def load(path):
    raw = path.read_bytes()
    return raw, json.loads(raw.decode("utf-8-sig"))


def time(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("expected UTC timestamp")
    return parsed


def seconds(duration):
    if duration.microseconds:
        raise ValueError("nonintegral seconds in M1 observation")
    return duration.days * 86_400 + duration.seconds


def number(value):
    if isinstance(value, float):
        raise ValueError("floating financial input")
    return Decimal(str(value))


def fmt(value, signed=False):
    return "n/a" if value is None else format(number(value), "+.4f" if signed else ".4f")


class Audit:
    def __init__(self):
        self.passed, self.proof_count = 0, 0
        self.failures, self.skipped, self.net_rr, self.target_met = [], [], [], []

    def check(self, condition, label):
        if condition:
            self.passed += 1
        else:
            self.failures.append(label)


class SourceCandles:
    """Original normalized candles, loaded lazily only if admission proofs exist."""
    def __init__(self, directory, manifest):
        self.directory, self.series = directory, {}
        self.metadata = {row["symbol"]: row for row in manifest["instruments"]}

    def candle(self, symbol, opened_at):
        if symbol not in self.series:
            path = (self.directory / self.metadata[symbol]["candles_file"]).resolve()
            if not path.is_relative_to(self.directory.resolve()):
                raise ValueError("source path escapes dataset")
            _, bars = load(path)
            self.series[symbol] = (time(bars[0]["open_time"]), bars)
        first, bars = self.series[symbol]
        offset = seconds(opened_at - first)
        if offset < 0 or offset % 60 or offset // 60 >= len(bars):
            raise ValueError("candle outside aligned original source range")
        result = bars[offset // 60]
        if time(result["open_time"]) != opened_at:
            raise ValueError("source dataset has a gap")
        return result


def held(anchor, candle):
    value = number(anchor["price"])
    return number(candle["high"]) <= value if anchor["kind"] == "high" else number(candle["low"]) >= value


def audit_candle(audit, label, candle, sources):
    start, end = time(candle["open_time"]), time(candle["close_time"])
    audit.check(end - start == MINUTE and start.second == 0 and start.microsecond == 0
                and candle["is_closed"] and candle["timeframe"] == "1m", label + ": closed M1")
    audit.check(sources.candle(candle["instrument"]["venue_symbol"], start) == candle,
                label + ": exact original candle")


def audit_anchor(audit, label, anchor, sweep_open, model, sources):
    opened, confirmed = time(anchor["candle_open_time"]), time(anchor["confirmed_at"])
    radius = {"three_candle": 1, "five_candle": 2}[model]
    audit.check(anchor["model"] == model and anchor["timeframe"] == "1m"
                and confirmed == opened + MINUTE * (radius + 1)
                and confirmed <= sweep_open and timedelta(0) <= sweep_open - opened <= timedelta(minutes=60),
                label + ": prior confirmation/model/age")
    symbol, column = anchor["instrument"]["venue_symbol"], "high" if anchor["kind"] == "high" else "low"
    pivot_price = number(anchor["price"])
    audit.check(number(sources.candle(symbol, opened)[column]) == pivot_price, label + ": original pivot price")
    adjacent = [number(sources.candle(symbol, opened + MINUTE * offset)[column])
                for offset in range(-radius, radius + 1) if offset]
    audit.check(all(pivot_price > value if column == "high" else pivot_price < value for value in adjacent),
                label + ": genuine strict original-candle fractal")


def audit_unbroken(audit, label, anchor, end_exclusive, sources):
    at, intact = time(anchor["confirmed_at"]), True
    while at < end_exclusive:
        intact = intact and held(anchor, sources.candle(anchor["instrument"]["venue_symbol"], at))
        at += MINUTE
    audit.check(intact, label + ": unconsumed throughout intervening source candles")


def net_rr(order, profile, metadata):
    entry, stop, target, quantity = (number(order[name]) for name in
        ("entry_price", "stop_loss_price", "take_profit_price", "quantity"))
    tick = number(metadata["price_tick"])
    fee, slip = number(profile["fee_bps_per_side"]) / 10_000, number(profile["stop_slippage_bps"]) / 10_000
    bullish = order["direction"] == "bullish"
    factor = Decimal(1) - slip if bullish else Decimal(1) + slip
    stop_exec = (stop * factor / tick).to_integral_value(rounding=ROUND_FLOOR if bullish else ROUND_CEILING) * tick
    risk_move, reward_move = (entry - stop_exec, target - entry) if bullish else (stop_exec - entry, entry - target)
    risk = risk_move * quantity + (entry + stop_exec) * quantity * fee
    reward = reward_move * quantity - (entry + target) * quantity * fee
    if quantity <= 0 or risk <= 0:
        raise ValueError("nonpositive sized quantity or stop risk")
    return reward / risk, reward, risk


def audit_proof(audit, label, proof, placement, profile, sources):
    intent, witness, order = proof["intent"], proof["smt"], proof["order"]
    sweep = intent["liquidity_sweep"]
    focus, peer, admission_peer = witness["focus_candle"], witness["peer_candle"], proof["peer_at_admission"]
    fa, pa = witness["focus_anchor"], witness["peer_anchor"]
    approved = time(proof["approved_at"])
    focus_open, focus_close = time(focus["open_time"]), time(focus["close_time"])
    direction, symbol = intent["direction"], intent["instrument"]["venue_symbol"]
    audit.check({symbol, pa["instrument"]["venue_symbol"]} == {"BTCUSDT", "ETHUSDT"}, label + ": BTC ETH pair")
    audit.check(intent["id"] == order["intent_id"] == placement["order"]["intent_id"]
                and order == placement["order"] and placement["symbol"] == symbol
                and time(placement["at"]) == approved, label + ": exact placed order")
    audit.check(witness["focus_sweep"] == sweep and sweep["instrument"] == intent["instrument"]
                == focus["instrument"] == fa["instrument"]
                and peer["instrument"] == pa["instrument"] == admission_peer["instrument"]
                and witness["direction"] == direction == order["direction"], label + ": exact sweep/instrument/direction")
    audit.check(time(sweep["sweep_candle_open_time"]) == focus_open
                and time(sweep["detected_at"]) == focus_close == time(witness["detected_at"])
                and focus_open == time(peer["open_time"]) and focus_close == time(peer["close_time"]),
                label + ": simultaneous sweep observations")
    expiry = time(witness["expires_at"])
    audit.check(expiry == focus_close + timedelta(minutes=12) and focus_close <= approved < expiry
                and time(intent["expires_at"]) == time(order["expires_at"]) > approved,
                label + ": exclusive evidence/order expiry")
    model = profile["smt_research"]["fractal_model"]
    for suffix, anchor in (("focus", fa), ("peer", pa)):
        audit_anchor(audit, label + " " + suffix, anchor, focus_open, model, sources)
    audit.check(fa["kind"] == pa["kind"]
                and abs(time(fa["candle_open_time"]) - time(pa["candle_open_time"])) <= timedelta(minutes=3),
                label + ": corresponding same-kind pivots")
    audit.check(number(sweep["reference_price"]) == number(fa["price"]), label + ": swept pivot reference")
    bullish = direction == "bullish"
    genuine = (sweep["side"] == "sell_side" and fa["kind"] == "low"
               and number(focus["low"]) == number(sweep["sweep_price"]) < number(fa["price"])
               and number(focus["close"]) >= number(fa["price"])) if bullish else (
               sweep["side"] == "buy_side" and fa["kind"] == "high"
               and number(focus["high"]) == number(sweep["sweep_price"]) > number(fa["price"])
               and number(focus["close"]) <= number(fa["price"]))
    audit.check(genuine and held(pa, peer) and held(pa, admission_peer), label + ": actual sweep with peer hold")
    trigger = intent["trigger_candle"]
    audit.check(approved == time(trigger["close_time"]) == time(admission_peer["close_time"])
                and time(trigger["open_time"]) == time(admission_peer["open_time"]), label + ": synchronized admission")
    for suffix, candle in (("focus", focus), ("peer", peer), ("trigger", trigger), ("admission peer", admission_peer)):
        audit_candle(audit, label + " " + suffix, candle, sources)
    audit_unbroken(audit, label + " focus before sweep", fa, focus_open, sources)
    audit_unbroken(audit, label + " peer before admission", pa, approved, sources)
    moment, intact = focus_close, True
    while moment < approved:
        candle = sources.candle(symbol, moment)
        intact = intact and (number(candle["low"]) >= number(sweep["sweep_price"]) if bullish
                             else number(candle["high"]) <= number(sweep["sweep_price"]))
        moment += MINUTE
    audit.check(intact, label + ": original sweep extreme remains valid")
    fvg, mss = intent["fair_value_gap"], intent["market_structure_shift"]
    formation = [time(value) for value in fvg["formed_from"]]
    audit.check(intent["trigger_mode"] == "fvg_confirmation" and fvg["state"]["status"] == "fresh"
                and intent["entry_mode"] == "consequent_encroachment"
                and formation[1] == time(mss["confirming_candle_open_time"])
                and formation[2] == time(trigger["open_time"])
                and formation[1] - formation[0] == formation[2] - formation[1] == MINUTE
                and time(fvg["formed_at"]) == approved and mss["direction"] == fvg["direction"] == direction,
                label + ": genuine causal MSS-middle FVG confirmation")
    left, right = sources.candle(symbol, formation[0]), sources.candle(symbol, formation[2])
    lower, upper = number(fvg["zone"]["lower"]), number(fvg["zone"]["upper"])
    audit.check((lower == number(left["high"]) and upper == number(right["low"]) if bullish
                 else lower == number(right["high"]) and upper == number(left["low"])) and lower < upper,
                label + ": original-candle FVG boundaries")
    metadata, raw_entry = sources.metadata[symbol], (lower + upper) / 2
    tick = number(metadata["price_tick"])
    rounded = (raw_entry / tick).to_integral_value(rounding=ROUND_FLOOR if bullish else ROUND_CEILING) * tick
    audit.check(number(order["entry_price"]) == rounded
                and (rounded < number(trigger["close"]) if bullish else rounded > number(trigger["close"]))
                and (number(order["stop_loss_price"]) < number(sweep["sweep_price"]) if bullish
                     else number(order["stop_loss_price"]) > number(sweep["sweep_price"])),
                label + ": rounded passive CE and strict sweep stop")
    ratio, reward, risk = net_rr(order, profile, metadata)
    audit.check(reward >= 2 * risk, label + ": independently recomputed net RR >= 2")
    audit.net_rr.append(ratio)
    completed = proof["m5_completed_at"]
    audit.check(completed is None or (time(completed) <= approved and time(completed).minute % 5 == 0),
                label + ": no future M5 close")
    if profile["smt_research"]["require_m5_alignment"]:
        audit.check(proof["m5_bias"] == direction and completed is not None, label + ": required M5 alignment")
    audit.proof_count += 1


def audit_frequency(audit, label, row):
    metrics, frequency = row["metrics"], row["frequency"]
    fills, closes = metrics["filled_entries"], metrics["completed_trades"]
    days, daily = number(frequency["elapsed_days"]), frequency["daily"]
    audit.check(frequency["filled_entries"] == fills and frequency["closed_trades"] == closes
                and sum(day["filled_entries"] for day in daily) == fills
                and sum(day["closed_trades"] for day in daily) == closes, label + ": frequency accounting")
    observed = Decimal(seconds(time(row["last_live_close"]) - time(row["first_live_open"]))) / 86_400
    audit.check(days > 0 and abs(days - observed) <= Decimal("1e-25")
                and abs(number(frequency["filled_entries_per_elapsed_day"]) - Decimal(fills) / days) <= Decimal("1e-25"),
                label + ": elapsed frequency denominator")
    audit.check(frequency["meets_average_frequency_target"] == (Decimal(fills) > 10 * days)
                and frequency["target_filled_entries_per_elapsed_day_exclusive"] == "10"
                and frequency["calendar_days_with_more_than_ten_fills"] == sum(day["filled_entries"] > 10 for day in daily),
                label + ": exclusive greater-than-ten target")
    active, streak, longest = sum(day["filled_entries"] > 0 for day in daily), 0, 0
    for day in daily:
        streak = streak + 1 if day["filled_entries"] == 0 else 0
        longest = max(longest, streak)
    audit.check(frequency["entry_active_days"] == active and frequency["calendar_days_observed"] == len(daily)
                and frequency["days_without_entry"] == len(daily) - active
                and frequency["max_consecutive_days_without_entry"] == longest, label + ": zero-entry days retained")


def audit_report(audit, directory):
    raw, report = load(directory / "evaluation-smt.json")
    _, prior = load(directory / "evaluation-entry-v2.json")
    _, manifest = load(directory / "manifest.json")
    sources = SourceCandles(directory, manifest)
    profiles, rows = report["configuration"]["profiles"], report["results"]
    by_name = {profile["name"]: profile for profile in profiles}
    expected = [profile["name"] for profile in prior["configuration"]["profiles"]] + list(SMT_NAMES)
    audit.check(report["report_version"] == 4 and report["status"] == "UNVALIDATED", directory.name + ": version/status")
    audit.check(len(profiles) == 34 and [profile["name"] for profile in profiles] == expected, directory.name + ": 34 profiles")
    audit.check(len(rows) == 68 and [(row["profile"], row["segment"]) for row in rows]
                == [(name, segment) for name in expected for segment in SEGMENTS], directory.name + ": 68 ordered segments")
    audit.check(report["provenance"] == prior["provenance"], directory.name + ": baseline data provenance")
    for old, new in zip(prior["results"], rows[:36], strict=True):
        for field in FIELDS:
            audit.check(old[field] == new[field], f"{directory.name}: legacy {old['profile']}/{old['segment']}/{field}")
    for source in report["compiled_sources"]:
        audit.check(hashlib.sha256((ROOT / source["file"]).read_bytes()).hexdigest() == source["sha256"],
                    directory.name + ": source " + source["file"])
    print(f"\nDATASET {directory.name}")
    print("SMT profile | segment | placed/filled/closed | fills/day | active/zero days | equity PnL | PF | open | >10/day")
    for row in rows:
        label = f"{directory.name}/{row['profile']}/{row['segment']}"
        profile = by_name[row["profile"]]
        audit_frequency(audit, label, row)
        if not row["profile"].startswith("smt_"):
            audit.check(profile["smt_research"] is None and row["smt_research"] is None, label + ": legacy no SMT gate")
            continue
        audit.check(profile["smt_required"] and profile["minimum_net_rr"] == "2"
                    and profile["risk_per_trade"] == "0.01" and profile["max_total_risk"] == "0.03"
                    and profile["entry_window"] == "always" and profile["target_policy"] == "nearest_viable",
                    label + ": mandatory SMT and unchanged risk")
        proof_rows = row["smt_research"]["admissions"]
        proofs = {proof["intent"]["id"]: proof for proof in proof_rows}
        placements = [event for event in row["trade_events"] if event["kind"] == "entry_placed"]
        placed = {event["order"]["intent_id"]: event for event in placements}
        admitted = sum(symbol["live_counters"]["admitted"] for symbol in row["diagnostics"])
        audit.check(len(proofs) == len(proof_rows) == len(placed) == len(placements) == admitted == row["metrics"]["placed_entries"]
                    and set(proofs) == set(placed), label + ": proof for every admission")
        fills = [event for event in row["trade_events"]
                 if event["kind"] == "execution" and event["event"]["event"] == "entry_filled"]
        audit.check(len(fills) == row["metrics"]["filled_entries"]
                    and all(event["event"]["intent_id"] in proofs for event in fills), label + ": proof for every fill")
        for identity, proof in proofs.items():
            audit_proof(audit, label + "/" + identity, proof, placed[identity], profile, sources)
        for event in fills:
            proof = proofs[event["event"]["intent_id"]]
            embargo = 2 if profile["fill_timing"] == "skip_first_eligible_bar" else 1
            audit.check(time(event["event"]["at"]) >= time(proof["approved_at"]) + MINUTE * embargo,
                        label + ": post-confirmation fill timing")
        frequency, metrics = row["frequency"], row["metrics"]
        if frequency["meets_average_frequency_target"]:
            audit.target_met.append(label)
        print(f"{row['profile']} | {row['segment']} | {metrics['placed_entries']}/{metrics['filled_entries']}/{metrics['completed_trades']} | "
              f"{fmt(frequency['filled_entries_per_elapsed_day'])} | {frequency['entry_active_days']}/{frequency['days_without_entry']} | "
              f"{fmt(metrics['net_equity_pnl'], True)} | {fmt(metrics['net_profit_factor'])} | {metrics['open_positions_at_end']} | "
              f"{frequency['meets_average_frequency_target']}")
    repeat = directory / "evaluation-smt-repeat.json"
    if repeat.is_file():
        audit.check(raw == repeat.read_bytes(), directory.name + ": repeated bytes identical")
    else:
        audit.skipped.append(directory.name + ": repeat report missing")
    print("sha256:", hashlib.sha256(raw).hexdigest())


def main():
    missing = [str(directory / "evaluation-smt.json") for directory in DIRECTORIES
               if not (directory / "evaluation-smt.json").is_file()]
    if missing:
        print("Reports unavailable; no audit executed:\n" + "\n".join(missing))
        return 2
    audit = Audit()
    with localcontext() as context:
        context.prec = 80
        for directory in DIRECTORIES:
            try:
                audit_report(audit, directory)
            except (OSError, ValueError, KeyError, TypeError, IndexError, ArithmeticError) as error:
                audit.failures.append(f"{directory.name}: malformed evidence or audit error: {error}")
    print(f"\nVERIFICATION {'PASS' if not audit.failures else 'FAIL'}: {audit.passed} checks passed; "
          f"{len(audit.failures)} failed; {len(audit.skipped)} optional repeat checks skipped; "
          f"{audit.proof_count} admission proofs independently examined.")
    if audit.net_rr:
        print(f"Minimum independently recomputed admitted net RR: {min(audit.net_rr):.8f}")
    print("Frequency target (>10 actual fills/day):", "MET in " + str(len(audit.target_met)) + " SMT profile-segments"
          if audit.target_met else "NOT ACHIEVED by any evaluated SMT profile-segment")
    for issue in audit.failures:
        print("FAIL:", issue)
    for issue in audit.skipped:
        print("SKIP:", issue)
    print("M5 audit checks recorded direction/timing, not a full independent reconstruction of the M5 state machine.")
    print("Net RR is the admitted estimate with assumed two-sided fees and adverse stop execution; gaps/funding are not modeled.")
    print("High frequency does not establish profitability; equity PnL includes open-position marks.")
    return 1 if audit.failures else 0


if __name__ == "__main__":
    sys.exit(main())
