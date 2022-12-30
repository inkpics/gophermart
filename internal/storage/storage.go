package storage

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type Storage struct {
	sqlDB           *sqlx.DB
	ErrDuplicateKey error
}

type order struct {
	ID         string  `db:"id"`
	Login      string  `db:"login"`
	Number     string  `db:"number"`
	Status     string  `db:"status"`
	Accrual    float64 `db:"accrual"`
	UploadedAt string  `db:"uploaded_at"`
}

type balance struct {
	ID        string  `db:"id"`
	Login     string  `db:"login"`
	Current   float64 `db:"current"`
	Withdrawn float64 `db:"withdrawn"`
}

type withdraw struct {
	ID          string  `db:"id"`
	Login       string  `db:"login"`
	OrderNumber string  `db:"order_number"`
	Sum         float64 `db:"sum"`
	ProcessedAt string  `db:"processed_at"`
}

func New(databaseAddr string) (*Storage, error) {
	db, err := sqlx.Connect("postgres", databaseAddr)
	if err != nil {
		return nil, fmt.Errorf("db connect: %w", err)
	}

	s := &Storage{
		sqlDB:           db,
		ErrDuplicateKey: fmt.Errorf("duplicate key"),
	}

	s.sqlDB.MustExec(`
		CREATE EXTENSION IF NOT EXISTS pgcrypto;

        CREATE TABLE IF NOT EXISTS gom_users (
            id text primary key,
            login text unique,
            password text
        );

        CREATE TABLE IF NOT EXISTS gom_orders (
            id text primary key,
            login text,
            number text unique,
            status text,
            accrual double precision,
            uploaded_at timestamp with time zone
        );

        CREATE TABLE IF NOT EXISTS gom_balances (
            id text primary key,
            login text,
            current double precision,
            withdrawn double precision
        );

        CREATE TABLE IF NOT EXISTS gom_withdrawals (
            id text primary key,
            login text,
            order_number text,
            sum double precision,
            processed_at timestamp with time zone
        );
    `)

	return s, nil
}

func (s *Storage) UserRegister(login, password string) error {
	tx, err := s.sqlDB.Begin()
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
	}

	_, err = tx.Exec("INSERT INTO gom_users VALUES (gen_random_uuid(), $1, $2)", login, password)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			if err.Code == "23505" {
				return s.ErrDuplicateKey
			}
		}
		return fmt.Errorf("db error: %w", err)
	}

	_, err = tx.Exec("INSERT INTO gom_balances VALUES (gen_random_uuid(), $1, 0, 0)", login)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			if err.Code == "23505" {
				return s.ErrDuplicateKey
			}
		}
		return fmt.Errorf("db error: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit error: %w", err)
	}

	return nil
}

func (s *Storage) UserLogin(login, password string) error {
	var count int
	err := s.sqlDB.QueryRowx("SELECT COUNT(*) FROM gom_users WHERE login = $1 AND password = $2", login, password).Scan(&count)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	if count < 1 {
		return fmt.Errorf("no credentials matches: %w", err)
	}

	return nil
}

func (s *Storage) OrderRegistered(login, orderNumber string) (int, error) {
	o := order{}
	err := s.sqlDB.QueryRowx("SELECT * FROM gom_orders WHERE number = $1 LIMIT 1", orderNumber).StructScan(&o)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("read rows: %w", err)
	}
	if o.Login == login {
		return 1, nil
	} else if o.Login != "" {
		return -1, nil
	}

	return 0, nil
}

func (s *Storage) OrderRegister(login, orderNumber string) error {
	_, err := s.sqlDB.Exec("INSERT INTO gom_orders VALUES (gen_random_uuid(), $1, $2, 'NEW', 0, NOW())", login, orderNumber)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			if err.Code == "23505" {
				return s.ErrDuplicateKey
			}
		}
		return fmt.Errorf("db error: %w", err)
	}

	return nil
}

func (s *Storage) Orders(login string) ([]order, error) {
	var result []order

	o := order{}
	rows, err := s.sqlDB.Queryx("SELECT * FROM gom_orders WHERE login =$1", login)
	if err != nil {
		return result, fmt.Errorf("read rows: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.StructScan(&o)
		if err != nil {
			return result, fmt.Errorf("rows struct scan: %w", err)
		}
		result = append(result, o)
	}

	err = rows.Err()
	if err != nil {
		return result, fmt.Errorf("rows error: %w", err)
	}

	return result, nil
}

func (s *Storage) UserBalance(login string) (balance, error) {
	b := balance{}

	err := s.sqlDB.QueryRowx("SELECT * FROM gom_balances WHERE login = $1 LIMIT 1", login).StructScan(&b)
	if err != nil {
		return b, fmt.Errorf("read rows: %w", err)
	}

	return b, nil
}

func (s *Storage) Withdraw(login, orderNumber string, sum float64) (int, error) {
	b := balance{}
	err := s.sqlDB.QueryRowx("SELECT * FROM gom_balances WHERE login = $1 LIMIT 1", login).StructScan(&b)
	if err != nil {
		return 0, fmt.Errorf("read rows: %w", err)
	}
	if b.Current < sum {
		return 402, nil
	}

	tx, err := s.sqlDB.Begin()
	if err != nil {
		return 0, fmt.Errorf("tx error: %w", err)
	}

	_, err = tx.Exec("UPDATE gom_balances SET current = $1, withdrawn = $2 WHERE login = $3", b.Current-sum, b.Withdrawn+sum, login)
	if err != nil {
		return 0, fmt.Errorf("db error: %w", err)
	}

	_, err = tx.Exec("INSERT INTO gom_withdrawals VALUES (gen_random_uuid(), $1, $2, $3, NOW())", login, orderNumber, sum)
	if err != nil {
		return 0, fmt.Errorf("db error: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return 0, fmt.Errorf("commit error: %w", err)
	}

	return 0, nil
}

func (s *Storage) Withdrawals(login string) ([]withdraw, error) {
	var result []withdraw

	wd := withdraw{}
	rows, err := s.sqlDB.Queryx("SELECT * FROM gom_withdrawals")
	if err != nil {
		return result, fmt.Errorf("read rows: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.StructScan(&wd)
		if err != nil {
			return result, fmt.Errorf("rows struct scan: %w", err)
		}
		result = append(result, wd)
	}

	err = rows.Err()
	if err != nil {
		return result, fmt.Errorf("rows error: %w", err)
	}

	return result, nil
}

func (s *Storage) OrdersProcessing() ([]order, error) {
	var result []order

	_, err := s.sqlDB.Exec("UPDATE gom_orders SET status = 'PROCESSING' WHERE status = 'NEW'")
	if err != nil {
		return result, fmt.Errorf("db update error: %w", err)
	}

	o := order{}
	rows, err := s.sqlDB.Queryx("SELECT * FROM gom_orders WHERE status = 'PROCESSING'")
	if err != nil {
		return result, fmt.Errorf("read rows: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.StructScan(&o)
		if err != nil {
			return result, fmt.Errorf("rows struct scan: %w", err)
		}
		result = append(result, o)
	}

	err = rows.Err()
	if err != nil {
		return result, fmt.Errorf("rows error: %w", err)
	}

	return result, nil
}

func (s *Storage) SetOrderInvalid(orderNumber string) error {
	_, err := s.sqlDB.Exec("UPDATE gom_orders SET status = 'INVALID' WHERE number = $1", orderNumber)
	if err != nil {
		return fmt.Errorf("db update error: %w", err)
	}

	return nil
}

func (s *Storage) UserFromOrderNumber(orderNumber string) (string, error) {
	var user string
	err := s.sqlDB.QueryRowx("SELECT login FROM gom_orders WHERE number = $1 LIMIT 1", orderNumber).Scan(&user)
	if err != nil {
		return "", fmt.Errorf("read rows: %w", err)
	}

	return user, nil
}

func (s *Storage) SetOrderProcessed(orderNumber string, accrual float64) error {
	login, err := s.UserFromOrderNumber(orderNumber)
	if err != nil {
		return fmt.Errorf("balance update user error: %w", err)
	}

	b := balance{}
	err = s.sqlDB.QueryRowx("SELECT * FROM gom_balances WHERE login = $1 LIMIT 1", login).StructScan(&b)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}

	tx, err := s.sqlDB.Begin()
	if err != nil {
		return fmt.Errorf("tx error: %w", err)
	}

	_, err = tx.Exec("UPDATE gom_orders SET status = 'PROCESSED', accrual = $1 WHERE number = $2", accrual, orderNumber)
	if err != nil {
		return fmt.Errorf("db update error: %w", err)
	}

	_, err = tx.Exec("UPDATE gom_balances SET current = $1 WHERE login = $2", b.Current+accrual, login)
	if err != nil {
		return fmt.Errorf("db error: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("commit error: %w", err)
	}

	return nil
}
