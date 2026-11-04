package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"go.kenn.io/agentsview/internal/db"
)

// SaveFrictionIssueLink records l, replacing any link for its pattern.
func (s *Store) SaveFrictionIssueLink(ctx context.Context, l db.FrictionIssueLink) error {
	_, err := s.pg.ExecContext(ctx, `
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
	return nil
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

// frictionIssueColumns are the link columns a pattern read selects through
// its LEFT JOIN; an unlinked pattern reads them all as NULL.
const frictionIssueColumns = `l.kata_instance_uid, l.kata_project_uid,
	l.issue_uid, l.qualified_id, l.web_url, l.source, l.linked_at`

// frictionIssueSelect returns the link columns and join a pattern read
// adds. A read-only role reading a schema that predates the links table
// cannot create it, so its pattern reads show every pattern unlinked
// instead of failing. The table is looked up until a read finds it, so a
// later migration takes effect without a restart.
func (s *Store) frictionIssueSelect(ctx context.Context) (columns, join string) {
	if !s.runtimeState().frictionIssueLinksPresent.Load() {
		if !pgHasTable(ctx, s.pg, "friction_issue_links") {
			return `NULL, NULL, NULL, NULL, NULL, NULL, NULL`, ""
		}
		s.runtimeState().frictionIssueLinksPresent.Store(true)
	}
	return frictionIssueColumns, `LEFT JOIN friction_issue_links l ON l.fingerprint = p.fingerprint`
}

type frictionIssueScan struct {
	instance, project, issue, qualified, url, source sql.NullString
	linkedAt                                         sql.NullTime
}

func (s *frictionIssueScan) targets() []any {
	return []any{&s.instance, &s.project, &s.issue, &s.qualified, &s.url, &s.source, &s.linkedAt}
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

var _ db.FrictionIssueLinkWriter = (*Store)(nil)
