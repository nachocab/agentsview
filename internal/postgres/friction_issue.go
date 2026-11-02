package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"go.kenn.io/agentsview/internal/db"
)

// SaveFrictionIssueLink records l, replacing any link for its pattern, and
// clears the pattern's filing state.
func (s *Store) SaveFrictionIssueLink(ctx context.Context, l db.FrictionIssueLink) error {
	tx, err := s.beginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("saving friction issue link: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM friction_filing_state WHERE fingerprint = $1`, l.Fingerprint); err != nil {
		return fmt.Errorf("clearing friction filing state: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO friction_issue_links (fingerprint, kata_instance_uid,
			kata_project_uid, issue_uid, qualified_id, web_url, source, linked_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (fingerprint) DO UPDATE SET
			kata_instance_uid = EXCLUDED.kata_instance_uid,
			kata_project_uid = EXCLUDED.kata_project_uid,
			issue_uid = EXCLUDED.issue_uid,
			qualified_id = EXCLUDED.qualified_id,
			web_url = EXCLUDED.web_url,
			source = EXCLUDED.source,
			linked_at = EXCLUDED.linked_at`,
		l.Fingerprint, l.KataInstanceUID, l.KataProjectUID, l.IssueUID,
		l.QualifiedID, l.WebURL, l.Source, l.LinkedAt.UTC())
	if err != nil {
		return fmt.Errorf("saving friction issue link: %w", err)
	}
	return tx.Commit()
}

// SaveFrictionFilingState records st, replacing any state for its pattern.
func (s *Store) SaveFrictionFilingState(ctx context.Context, st db.FrictionFilingState) error {
	_, err := s.pg.ExecContext(ctx, `
		INSERT INTO friction_filing_state (fingerprint, state, attempts,
			first_failed_at, next_attempt_at, last_error, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (fingerprint) DO UPDATE SET
			state = EXCLUDED.state, attempts = EXCLUDED.attempts,
			first_failed_at = EXCLUDED.first_failed_at,
			next_attempt_at = EXCLUDED.next_attempt_at,
			last_error = EXCLUDED.last_error, updated_at = EXCLUDED.updated_at`,
		st.Fingerprint, st.State, st.Attempts, utcPtr(st.FirstFailedAt),
		utcPtr(st.NextAttemptAt), st.LastError, st.UpdatedAt.UTC())
	if err != nil {
		return fmt.Errorf("saving friction filing state: %w", err)
	}
	return nil
}

func utcPtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC()
}

// DeleteFrictionIssueLink removes the local link only; the Kata issue stays.
func (s *Store) DeleteFrictionIssueLink(ctx context.Context, fingerprint string) (bool, error) {
	res, err := s.pg.ExecContext(ctx,
		`DELETE FROM friction_issue_links WHERE fingerprint = $1`, fingerprint)
	if err != nil {
		return false, fmt.Errorf("deleting friction issue link: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("deleting friction issue link: %w", err)
	}
	return n > 0, nil
}

// frictionLinkColumns and frictionFilingColumns are what a pattern read
// selects through its LEFT JOINs; absent rows read as NULL.
const (
	frictionLinkColumns = `l.kata_instance_uid, l.kata_project_uid,
	l.issue_uid, l.qualified_id, l.web_url, l.source, l.linked_at`
	frictionFilingColumns = `f.state, f.attempts, f.first_failed_at,
	f.next_attempt_at, f.last_error, f.updated_at`
)

// frictionIssueSelect returns the link and filing-state columns and joins a
// pattern read adds. A read-only role reading a schema that predates either
// table cannot create it, so its pattern reads show that table's fields as
// empty instead of failing. Each table is looked up until a read finds it,
// so a later migration takes effect without a restart.
func (s *Store) frictionIssueSelect(ctx context.Context) (columns, join string) {
	rt := s.runtimeState()
	link, linkJoin := frictionLinkColumns, `LEFT JOIN friction_issue_links l ON l.fingerprint = p.fingerprint`
	if !s.frictionTablePresent(ctx, &rt.frictionIssueLinksPresent, "friction_issue_links") {
		link, linkJoin = `NULL, NULL, NULL, NULL, NULL, NULL, NULL`, ""
	}
	filing, filingJoin := frictionFilingColumns, `LEFT JOIN friction_filing_state f ON f.fingerprint = p.fingerprint`
	if !s.frictionTablePresent(ctx, &rt.frictionFilingStatePresent, "friction_filing_state") {
		filing, filingJoin = `NULL, NULL, NULL, NULL, NULL, NULL`, ""
	}
	return link + ", " + filing, linkJoin + " " + filingJoin
}

// frictionTablePresent reports whether table exists, remembering a yes in
// present.
func (s *Store) frictionTablePresent(ctx context.Context, present *atomic.Bool, table string) bool {
	if present.Load() {
		return true
	}
	if !pgHasTable(ctx, s.pg, table) {
		return false
	}
	present.Store(true)
	return true
}

type frictionIssueScan struct {
	instance, project, issue, qualified, url, source sql.NullString
	linkedAt                                         sql.NullTime

	state, lastError                  sql.NullString
	attempts                          sql.NullInt64
	firstFailed, nextAttempt, updated sql.NullTime
}

func (s *frictionIssueScan) targets() []any {
	return []any{
		&s.instance, &s.project, &s.issue, &s.qualified, &s.url, &s.source, &s.linkedAt,
		&s.state, &s.attempts, &s.firstFailed, &s.nextAttempt, &s.lastError, &s.updated,
	}
}

func (s *frictionIssueScan) filing(fingerprint string) *db.FrictionFilingState {
	if !s.state.Valid {
		return nil
	}
	return &db.FrictionFilingState{
		Fingerprint: fingerprint, State: s.state.String,
		Attempts: int(s.attempts.Int64), LastError: s.lastError.String,
		FirstFailedAt: nullTimePtr(s.firstFailed), NextAttemptAt: nullTimePtr(s.nextAttempt),
		UpdatedAt: s.updated.Time.UTC(),
	}
}

func nullTimePtr(t sql.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}
	return new(t.Time.UTC())
}

func (s *frictionIssueScan) link(fingerprint string) *db.FrictionIssueLink {
	if !s.issue.Valid {
		return nil
	}
	return &db.FrictionIssueLink{
		Fingerprint: fingerprint, KataInstanceUID: s.instance.String,
		KataProjectUID: s.project.String, IssueUID: s.issue.String,
		QualifiedID: s.qualified.String, WebURL: s.url.String,
		Source: s.source.String, LinkedAt: s.linkedAt.Time.UTC(),
	}
}

// FrictionIssueLinksWritable reports whether startup proved this role can
// write Kata issue links and automatic filing state. The filing routes refuse before contacting Kata
// when it cannot, so no issue is created without its link.
func (s *Store) FrictionIssueLinksWritable() bool {
	return s.runtimeState().frictionIssueLinksWritable.Load()
}

// probeFrictionIssueLinkWrite writes a link and a filing state inside a
// rolled-back transaction, as saving a link does. A missing table or a
// read-only role reports false.
func (s *Store) probeFrictionIssueLinkWrite(ctx context.Context) bool {
	if !pgHasTable(ctx, s.pg, "friction_issue_links") || !pgHasTable(ctx, s.pg, "friction_filing_state") {
		return false
	}
	tx, err := s.beginTx(ctx, nil)
	if err != nil {
		return false
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO friction_issue_links (fingerprint, kata_instance_uid,
			kata_project_uid, issue_uid, qualified_id, web_url, source, linked_at)
		VALUES ('friction-capability-probe', '', '', '', '', '', 'manual', NOW())
		ON CONFLICT (fingerprint) DO NOTHING`)
	if err == nil {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO friction_filing_state (fingerprint, state, updated_at)
			VALUES ('friction-capability-probe', 'paused', NOW())
			ON CONFLICT (fingerprint) DO NOTHING`)
	}
	if err != nil && !IsReadOnlyError(err) {
		log.Printf("friction: issue link capability probe: %v", err)
	}
	return err == nil
}

var _ db.FrictionIssueLinkWriter = (*Store)(nil)
