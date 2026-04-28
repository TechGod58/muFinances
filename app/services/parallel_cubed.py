from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        parsed = float(value)
        if math.isfinite(parsed):
            return parsed
    except (TypeError, ValueError):
        pass
    return fallback


def _bounded(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


@dataclass(frozen=True)
class FlowBatch:
    id: str
    phase: int
    title: str
    seed_region: str
    objective: str
    deliverables: list[str]
    dependencies: list[str]
    completion_signal: str


class FinanceParallelCubed:
    """Finance-native Parallel Cubed runtime for capability routing.

    This adapts the region/synapse/signal idea to muFinances without coupling the
    app to the muStatistics sidecar. It is intentionally deterministic and small:
    it maps a requested capability to active finance regions, bound modules, and
    guard decisions that later batches can use for workflow automation.
    """

    def __init__(self, genome_path: Path) -> None:
        self.genome_path = genome_path
        self.started_at = _now_ms()
        self.last_route_at: int | None = None
        self.route_count = 0
        self.energy: dict[str, float] = {}
        self.reload()

    def reload(self) -> None:
        payload = json.loads(self.genome_path.read_text(encoding='utf-8'))
        self.meta = payload.get('meta', {}) or {}
        self.params = payload.get('params', {}) or {}
        self.regions = [
            {'id': str(item.get('id', '')).strip(), 'label': str(item.get('label', '')).strip()}
            for item in payload.get('regions', [])
            if str(item.get('id', '')).strip()
        ]
        self.region_ids = [item['id'] for item in self.regions]
        self.synapses: dict[str, dict[str, float]] = {}
        for edge in payload.get('synapses', []):
            src = str(edge.get('src', '')).strip()
            dst = str(edge.get('dst', '')).strip()
            if src and dst:
                self.synapses.setdefault(src, {})[dst] = _bounded(_safe_float(edge.get('w'), 0.0), 0.0, 2.0)
        self.bindings = {
            str(region): [str(item) for item in bindings]
            for region, bindings in (payload.get('bindings', {}) or {}).items()
            if isinstance(bindings, list)
        }
        for region in self.region_ids:
            self.energy.setdefault(region, 0.0)

    def activate(self, seed_region: str, spread: float | None = None) -> list[str]:
        if not self.region_ids:
            return []
        seed = seed_region if seed_region in self.region_ids else self.region_ids[0]
        spread_value = _bounded(_safe_float(spread, _safe_float(self.params.get('spread'), 0.62)), 0.0, 1.0)
        max_active = max(1, min(12, int(_safe_float(self.params.get('max_active_regions'), 4))))
        active = [seed]
        neighbors = sorted((self.synapses.get(seed, {}) or {}).items(), key=lambda item: item[1], reverse=True)
        for neighbor, weight in neighbors:
            if len(active) >= max_active:
                break
            if weight * spread_value > 0.05:
                active.append(neighbor)
        return active

    def route(self, seed_region: str, intent: str = '', feedback: float = 0.0, entropy: float = 0.0) -> dict[str, Any]:
        active_regions = self.activate(seed_region)
        feedback_value = _bounded(feedback, -1.0, 1.0)
        entropy_value = _bounded(entropy, 0.0, 1.0)
        delta = 0.025 * feedback_value * (1.0 - entropy_value)
        for region in active_regions:
            self.energy[region] = _bounded(self.energy.get(region, 0.0) * 0.98 + 0.2 + delta, 0.0, 4.0)
        self.last_route_at = _now_ms()
        self.route_count += 1
        return {
            'intent': intent,
            'seedRegion': active_regions[0] if active_regions else seed_region,
            'activeRegions': active_regions,
            'bindings': {region: self.bindings.get(region, []) for region in active_regions},
            'energy': {region: round(self.energy.get(region, 0.0), 6) for region in active_regions},
            'adaptation': {'feedback': feedback_value, 'entropy': entropy_value, 'delta': round(delta, 6)},
        }

    def guard(self, telemetry: dict[str, Any]) -> dict[str, Any]:
        validation_errors = max(0, int(_safe_float(telemetry.get('validationErrors'), 0)))
        missing_controls = max(0, int(_safe_float(telemetry.get('missingControls'), 0)))
        stale_inputs = max(0, int(_safe_float(telemetry.get('staleInputs'), 0)))
        open_exceptions = max(0, int(_safe_float(telemetry.get('openExceptions'), 0)))
        high_variance_count = max(0, int(_safe_float(telemetry.get('highVarianceCount'), 0)))

        syndrome = [
            1 if validation_errors > 0 else 0,
            1 if missing_controls > 0 else 0,
            1 if stale_inputs > 0 else 0,
            1 if open_exceptions > 0 else 0,
            1 if high_variance_count > 0 else 0,
        ]
        risk = sum(syndrome) / len(syndrome)
        checkpoint_threshold = _bounded(_safe_float(self.params.get('risk_checkpoint_threshold'), 0.35), 0.0, 1.0)
        halt_threshold = _bounded(_safe_float(self.params.get('risk_halt_threshold'), 0.68), 0.0, 1.0)
        if risk >= halt_threshold:
            action = 'halt'
            level = 'critical'
        elif risk >= checkpoint_threshold:
            action = 'checkpoint'
            level = 'elevated'
        else:
            action = 'continue'
            level = 'normal'
        return {
            'decision': {'action': action, 'level': level, 'risk': round(risk, 6)},
            'syndrome': syndrome,
            'telemetryEcho': telemetry,
        }

    def status(self) -> dict[str, Any]:
        return {
            'ready': True,
            'genomePath': str(self.genome_path),
            'genomeId': self.meta.get('id'),
            'genomeVersion': self.meta.get('version'),
            'regions': self.regions,
            'params': self.params,
            'routeCount': self.route_count,
            'lastRouteAt': self.last_route_at,
            'energy': self.energy,
            'uptimeMs': max(0, _now_ms() - self.started_at),
        }


PHASE_BATCHES: list[FlowBatch] = [
    FlowBatch('B01', 1, 'Foundation ledger hardening', 'foundation', 'Make dimensional ledger, fiscal periods, dimensions, audit, and migrations the durable core.', ['ledger service', 'dimension hierarchy', 'migration runner', 'backup/restore hooks'], [], 'All current APIs run from the planning ledger.'),
    FlowBatch('B02', 1, 'Security and control baseline', 'security', 'Add identity, roles, row-level access, and masking before sensitive planning modules expand.', ['local auth', 'role permissions', 'row-level filters', 'sensitive compensation masking'], ['B01'], 'Every API can evaluate actor, role, and allowed dimensions.'),
    FlowBatch('B03', 2, 'Operating budget workspace', 'planning', 'Replace spreadsheet budget collection with governed department planning.', ['submission workflow', 'budget assumptions', 'transfers', 'one-time vs recurring lines'], ['B01', 'B02'], 'Departments can submit and route operating budgets.'),
    FlowBatch('B04', 2, 'Enrollment tuition planning', 'planning', 'Model headcount, rates, discounting, retention, and tuition revenue.', ['enrollment drivers', 'tuition rates', 'discounting', 'term forecast'], ['B03'], 'Tuition revenue flows into the ledger by scenario and period.'),
    FlowBatch('B05', 2, 'Workforce faculty grants capital', 'planning', 'Add the major campus planning subledgers.', ['position control', 'faculty load', 'grant budgets', 'capital requests'], ['B03'], 'Major campus planning lines post through controlled subledgers.'),
    FlowBatch('B06', 3, 'Forecast and scenario engine', 'drivers', 'Expand driver graph, scenario cloning, comparison, and forecast methods.', ['typed drivers', 'scenario compare', 'rolling forecast', 'confidence intervals'], ['B01', 'B03'], 'Scenario outputs can be compared and explained by driver lineage.'),
    FlowBatch('B07', 4, 'Reporting analytics layer', 'reporting', 'Build report definitions, dashboards, statements, and export packages.', ['report builder', 'dashboard builder', 'financial statements', 'scheduled exports'], ['B01', 'B06'], 'Board-ready reports run from saved definitions.'),
    FlowBatch('B08', 5, 'Close reconciliation consolidation', 'close', 'Add close checklist, account reconciliation, intercompany, and consolidation.', ['close calendar', 'reconciliation matching', 'eliminations', 'consolidation runs'], ['B01', 'B02', 'B07'], 'Close cycle can be tracked, reconciled, consolidated, and audited.'),
    FlowBatch('B09', 6, 'Campus integrations', 'integrations', 'Bring real source systems into controlled imports and exports.', ['CSV/XLSX import', 'connector jobs', 'sync logs', 'Power BI export'], ['B01', 'B02'], 'Imports have mappings, validation, rejection handling, and audit lineage.'),
    FlowBatch('B10', 7, 'Governed automation', 'automation', 'Add variance, anomaly, budget, and reconciliation assistants with human approval.', ['variance assistant', 'anomaly detection', 'budget assistant', 'match suggestions'], ['B06', 'B07', 'B08'], 'Automation produces cited recommendations and never posts silently.'),
    FlowBatch('B11', 8, 'Workspace UX completion', 'experience', 'Turn the prototype into role-specific campus finance workspaces.', ['module shell', 'planner workspace', 'controller workspace', 'executive dashboard'], ['B03', 'B07'], 'Users can complete core workflows without raw API knowledge.'),
    FlowBatch('B12', 9, 'Deployment operations', 'operations', 'Package, monitor, back up, and document campus-ready operation.', ['Windows service or Docker', 'health checks', 'restore tests', 'runbooks'], ['B01', 'B02', 'B09'], 'The app is recoverable, observable, and runnable on localhost/internal host.'),
]


def batches_as_dicts() -> list[dict[str, Any]]:
    return [
        {
            'id': item.id,
            'phase': item.phase,
            'title': item.title,
            'seedRegion': item.seed_region,
            'objective': item.objective,
            'deliverables': item.deliverables,
            'dependencies': item.dependencies,
            'completionSignal': item.completion_signal,
        }
        for item in PHASE_BATCHES
    ]


GENOME_PATH = Path(__file__).resolve().parent.parent / 'parallel_cubed_finance_genome.json'
finance_flow = FinanceParallelCubed(GENOME_PATH)
