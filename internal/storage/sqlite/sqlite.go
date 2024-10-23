package sqlite

import (
	"awesomeProject/internal/storage"
	"database/sql"
	"errors"
	"fmt"
	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
)

type Storage struct {
	db *sql.DB
}

func New(storagePath string) (*Storage, error) {
	const op = "storage.sqlite.New"
	db, err := sql.Open("sqlite3", storagePath)

	if err != nil {
		return nil, fmt.Errorf("%s: %w bla", op, err)
	}

	stmt, err := db.Prepare(`
		CREATE TABLE IF NOT EXISTS url(
				id INTEGER PRIMARY KEY,
				alias TEXT NOT NULL UNIQUE,
				url TEXT NOT NULL);
		CREATE INDEX IF NOT EXISTS idx_alias ON url(alias);
	`)
	if err != nil {
		return nil, fmt.Errorf("{op}: #{err} bla bla")
	}
	_, err = stmt.Exec()

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return &Storage{db: db}, nil
}

func (s *Storage) SaveURL(urlToSave string, alias string) (int64, error) {
	const op = "storage.sqlite.SaveURL"
	stmt, err := s.db.Prepare("INSERT INTO url(url,alias) VALUES(?,?)")
	if err != nil {
		return 0, fmt.Errorf("%s:%w", op, err)
	}

	res, err := stmt.Exec(urlToSave, alias)
	if err != nil {
		// Переделать - если ссылка уже есть, то выдаем ошибку
		if sqliteErr, ok := err.(sqlite3.Error); ok && errors.Is(sqliteErr.ExtendedCode, sqlite3.ErrConstraint) {
			return 0, fmt.Errorf("%s:%w", op, storage.ErrURLExist)
		}
		return 0, fmt.Errorf("%s:%w", op, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("%s:%w", op, err)
	}
	return id, nil
}

func (s *Storage) GetUrl(alias string) (string, error) {
	const op = "storage.sqlite.GetURL"
	stmt, err := s.db.Prepare("SELECT url FROM url WHERE alias = ?")
	if err != nil {
		return "", fmt.Errorf("%s:%w", op, err)
	}
	var res string

	err = stmt.QueryRow(alias).Scan(&res)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", storage.ErrURLNotFound
		}

		return "", fmt.Errorf("%s:%w", op, err)
	}
	return res, nil
}

func (s *Storage) DeleteURL(alias string) error {
	const op = "storage.sqlite"
	stmt, err := s.db.Prepare("DELETE FROM url WHERE alias = ?")
	if err != nil {
		return fmt.Errorf("%s:%w", op, err)
	}
	defer stmt.Close()

	res, err := stmt.Exec(alias)

	if err != nil {
		return fmt.Errorf("%s: exec statement error: %w", op, err)
	}

	rowsAffected, err := res.RowsAffected()

	if err != nil {
		return fmt.Errorf("%s: unable to fetch affected rows: %w", op, err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("%s: no record found to delete", op)
	}
	return nil
}
