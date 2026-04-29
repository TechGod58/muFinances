from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app import db
from app.routers.deps import require
from app.schemas import (
    ApprovalAction,
    BackupCreate,
    DimensionMemberCreate,
    EntityCommentCreate,
    EvidenceAttachmentCreate,
    FiscalPeriodCreate,
    JournalAdjustmentCreate,
    LedgerEntryCreate,
    LedgerReverseCreate,
)
from app.services.evidence import (
    create_attachment,
    create_comment,
    entity_evidence,
    list_attachments,
    list_comments,
    resolve_comment,
    status as evidence_status,
)
from app.services.foundation import (
    append_ledger_entry,
    create_backup,
    create_dimension_member,
    dimension_hierarchy,
    ensure_foundation_ready,
    foundation_status,
    list_backups,
    list_fiscal_periods,
    list_ledger_entries,
    list_migrations,
    reverse_ledger_entry,
    restore_backup,
    set_period_closed,
    upsert_fiscal_period,
)
from app.services.ledger_depth import (
    approve_journal_adjustment,
    create_journal_adjustment,
    ledger_basis_summary,
    list_journal_adjustments,
    reject_journal_adjustment,
    status as ledger_depth_status,
    submit_journal_adjustment,
)

router = APIRouter(tags=['ledger'])


@router.get('/api/ledger-depth/status')
def ledger_depth_status_endpoint() -> dict[str, Any]:
    return ledger_depth_status()


@router.get('/api/ledger-depth/basis-summary')
def ledger_depth_basis_summary(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    require(request, 'ledger.read')
    return ledger_basis_summary(scenario_id)


@router.get('/api/ledger-depth/journals')
def ledger_depth_journals(request: Request, scenario_id: int = Query(..., ge=1)) -> dict[str, Any]:
    require(request, 'ledger.read')
    rows = list_journal_adjustments(scenario_id)
    return {'scenario_id': scenario_id, 'count': len(rows), 'journals': rows}


@router.post('/api/ledger-depth/journals')
def ledger_depth_create_journal(payload: JournalAdjustmentCreate, request: Request) -> dict[str, Any]:
    require(request, 'ledger.write')
    try:
        return create_journal_adjustment(payload.model_dump(), request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post('/api/ledger-depth/journals/{journal_id}/submit')
def ledger_depth_submit_journal(journal_id: int, request: Request) -> dict[str, Any]:
    require(request, 'ledger.write')
    try:
        return submit_journal_adjustment(journal_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post('/api/ledger-depth/journals/{journal_id}/approve')
def ledger_depth_approve_journal(journal_id: int, request: Request) -> dict[str, Any]:
    require(request, 'ledger.write')
    try:
        return approve_journal_adjustment(journal_id, request.state.user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post('/api/ledger-depth/journals/{journal_id}/reject')
def ledger_depth_reject_journal(journal_id: int, payload: ApprovalAction, request: Request) -> dict[str, Any]:
    require(request, 'ledger.write')
    try:
        return reject_journal_adjustment(journal_id, request.state.user, payload.note)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get('/api/evidence/status')
def evidence_status_endpoint() -> dict[str, Any]:
    return evidence_status()


@router.get('/api/evidence/comments')
def evidence_comments(
    request: Request,
    entity_type: str | None = Query(None),
    entity_id: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    require(request, 'reports.read')
    rows = list_comments(entity_type, entity_id, limit)
    return {'count': len(rows), 'comments': rows}


@router.post('/api/evidence/comments')
def evidence_create_comment(payload: EntityCommentCreate, request: Request) -> dict[str, Any]:
    require(request, 'ledger.write')
    return create_comment(payload.model_dump(), request.state.user)


@router.post('/api/evidence/comments/{comment_id}/resolve')
def evidence_resolve_comment(comment_id: int, request: Request) -> dict[str, Any]:
    require(request, 'ledger.write')
    try:
        return resolve_comment(comment_id, request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/api/evidence/attachments')
def evidence_attachments(
    request: Request,
    entity_type: str | None = Query(None),
    entity_id: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    require(request, 'reports.read')
    rows = list_attachments(entity_type, entity_id, limit)
    return {'count': len(rows), 'attachments': rows}


@router.post('/api/evidence/attachments')
def evidence_create_attachment(payload: EvidenceAttachmentCreate, request: Request) -> dict[str, Any]:
    require(request, 'ledger.write')
    return create_attachment(payload.model_dump(), request.state.user)


@router.get('/api/evidence/entity')
def evidence_entity(request: Request, entity_type: str = Query(...), entity_id: str = Query(...)) -> dict[str, Any]:
    require(request, 'reports.read')
    return entity_evidence(entity_type, entity_id)


@router.get('/api/foundation/ledger')
def foundation_ledger(
    request: Request,
    scenario_id: int = Query(..., ge=1),
    include_reversed: bool = Query(False),
    limit: int = Query(500, ge=1, le=5000),
) -> dict[str, Any]:
    scenario = db.fetch_one('SELECT id FROM scenarios WHERE id = ?', (scenario_id,))
    if scenario is None:
        raise HTTPException(status_code=404, detail='Scenario not found.')
    require(request, 'ledger.read')
    rows = list_ledger_entries(scenario_id, include_reversed=include_reversed, limit=limit, user=request.state.user)
    return {'scenario_id': scenario_id, 'count': len(rows), 'entries': rows}


@router.post('/api/foundation/ledger')
def foundation_post_ledger(payload: LedgerEntryCreate, request: Request) -> dict[str, Any]:
    require(request, 'ledger.write')
    try:
        return append_ledger_entry(payload.model_dump(), actor=request.state.user['email'], user=request.state.user)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post('/api/foundation/ledger/{entry_id}/reverse')
def foundation_reverse_ledger(entry_id: int, payload: LedgerReverseCreate, request: Request) -> dict[str, Any]:
    require(request, 'ledger.reverse')
    try:
        return reverse_ledger_entry(entry_id, payload.reason, actor=request.state.user['email'])
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get('/api/foundation/fiscal-periods')
def foundation_fiscal_periods(fiscal_year: str | None = Query(None)) -> dict[str, Any]:
    periods = list_fiscal_periods(fiscal_year)
    return {'count': len(periods), 'periods': periods}


@router.post('/api/foundation/fiscal-periods')
def foundation_upsert_fiscal_period(payload: FiscalPeriodCreate, request: Request) -> dict[str, Any]:
    require(request, 'periods.manage')
    return upsert_fiscal_period(payload.model_dump(), actor=request.state.user['email'])


@router.post('/api/foundation/fiscal-periods/{period}/close')
def foundation_close_fiscal_period(period: str, request: Request) -> dict[str, Any]:
    require(request, 'periods.manage')
    try:
        return set_period_closed(period, True, actor=request.state.user['email'])
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post('/api/foundation/fiscal-periods/{period}/reopen')
def foundation_reopen_fiscal_period(period: str, request: Request) -> dict[str, Any]:
    require(request, 'periods.manage')
    try:
        return set_period_closed(period, False, actor=request.state.user['email'])
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/api/foundation/dimensions/hierarchy')
def foundation_dimension_hierarchy() -> dict[str, list[dict[str, Any]]]:
    return dimension_hierarchy()


@router.post('/api/foundation/dimensions')
def foundation_upsert_dimension(payload: DimensionMemberCreate, request: Request) -> dict[str, Any]:
    require(request, 'dimensions.manage')
    return create_dimension_member(payload.model_dump(), actor=request.state.user['email'])


@router.get('/api/foundation/migrations')
def foundation_migrations() -> dict[str, Any]:
    migrations = list_migrations()
    return {'count': len(migrations), 'migrations': migrations}


@router.post('/api/foundation/migrations/run')
def foundation_run_migrations() -> dict[str, Any]:
    ensure_foundation_ready()
    migrations = list_migrations()
    return {'applied': True, 'count': len(migrations), 'migrations': migrations}


@router.get('/api/foundation/status')
def foundation_status_endpoint() -> dict[str, Any]:
    return foundation_status()


@router.get('/api/foundation/backups')
def foundation_backups() -> dict[str, Any]:
    backups = list_backups()
    return {'count': len(backups), 'backups': backups}


@router.post('/api/foundation/backups')
def foundation_create_backup(payload: BackupCreate, request: Request) -> dict[str, Any]:
    require(request, 'backups.manage')
    return create_backup(note=payload.note, actor=request.state.user['email'])


@router.post('/api/foundation/backups/{backup_key}/restore')
def foundation_restore_backup(backup_key: str, request: Request) -> dict[str, Any]:
    require(request, 'backups.manage')
    try:
        return restore_backup(backup_key, actor=request.state.user['email'])
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
