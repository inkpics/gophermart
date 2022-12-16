package storage

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type Storage struct {
	DatabaseAddr    string
	ErrDuplicateKey error
}

type User struct {
	ID       string `db:"id"`
	Login    string `db:"login"`
	Password string `db:"password"`
}

type Order struct {
	ID         string  `db:"id"`
	Login      string  `db:"login"`
	Number     string  `db:"number"`
	Status     string  `db:"status"`
	Accrual    float64 `db:"accrual"`
	UploadedAt string  `db:"uploaded_at"`
}

type Balance struct {
	ID        string  `db:"id"`
	Login     string  `db:"login"`
	Current   float64 `db:"current"`
	Withdrawn float64 `db:"withdrawn"`
}

type Withdraw struct {
	ID          string  `db:"id"`
	Login       string  `db:"login"`
	OrderNumber string  `db:"order_number"`
	Sum         float64 `db:"sum"`
	ProcessedAt string  `db:"processed_at"`
}

func New(databaseAddr string) (*Storage, error) {
	s := &Storage{
		DatabaseAddr:    databaseAddr,
		ErrDuplicateKey: fmt.Errorf("duplicate key"),
	}

	db, err := sqlx.Connect("postgres", s.DatabaseAddr)
	if err != nil {
		return s, fmt.Errorf("db connect: %w", err)
	}
	defer db.Close()

	db.MustExec(`
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

	user := User{}
	rows, err := db.Queryx("SELECT * FROM gom_users")
	if err != nil {
		return s, fmt.Errorf("read rows: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.StructScan(&user)
		if err != nil {
			return s, fmt.Errorf("rows struct scan: %w", err)
		}
	}
	err = rows.Err()
	if err != nil {
		return s, fmt.Errorf("rows error: %w", err)
	}

	return s, nil
}

func uuidV4() string {
	id := uuid.New()
	return id.String()
}

func (s *Storage) UserRegister(login, password string) error {
	db, err := sqlx.Connect("postgres", s.DatabaseAddr)
	if err != nil {
		return fmt.Errorf("sql connect: %w", err)
	}
	defer db.Close()

	_, err = db.Exec("INSERT INTO gom_users VALUES ($1, $2, $3)", uuidV4(), login, password)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			if err.Code == "23505" {
				return s.ErrDuplicateKey
			}
		}
	}

	_, err = db.Exec("INSERT INTO gom_balances VALUES ($1, $2, 0, 0)", uuidV4(), login)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			if err.Code == "23505" {
				return s.ErrDuplicateKey
			}
		}
	}

	if err != nil {
		return fmt.Errorf("db error: %w", err)
	}
	return nil
}

func (s *Storage) UserLogin(login, password string) error {
	db, err := sqlx.Connect("postgres", s.DatabaseAddr)
	if err != nil {
		return fmt.Errorf("sql connect: %w", err)
	}
	defer db.Close()

	var count int
	rows, err := db.Queryx("SELECT COUNT(*) FROM gom_users WHERE login = $1 AND password = $2", login, password)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return fmt.Errorf("read rows: %w", err)
		}
		if count < 1 {
			return fmt.Errorf("no credentials matches: %w", err)
		}
	}
	err = rows.Err()
	if err != nil {
		return fmt.Errorf("rows error: %w", err)
	}

	return nil
}

func (s *Storage) OrderRegistered(login, orderNumber string) (int, error) {
	db, err := sqlx.Connect("postgres", s.DatabaseAddr)
	if err != nil {
		return 0, fmt.Errorf("sql connect: %w", err)
	}
	defer db.Close()

	order := Order{}
	rows, err := db.Queryx("SELECT * FROM gom_orders WHERE number = $1", orderNumber)
	if err != nil {
		return 0, fmt.Errorf("read rows: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.StructScan(&order); err != nil {
			return 0, fmt.Errorf("read rows: %w", err)
		}
		if order.Login == login {
			return 1, nil
		} else if order.Login != "" {
			return -1, nil
		}
	}
	err = rows.Err()
	if err != nil {
		return 0, fmt.Errorf("rows error: %w", err)
	}

	return 0, nil
}

func (s *Storage) OrderRegister(login, orderNumber string) error {
	db, err := sqlx.Connect("postgres", s.DatabaseAddr)
	if err != nil {
		return fmt.Errorf("sql connect: %w", err)
	}
	defer db.Close()

	_, err = db.Exec("INSERT INTO gom_orders VALUES ($1, $2, $3, 'NEW', 0, NOW())", uuidV4(), login, orderNumber)
	if err != nil {
		if err, ok := err.(*pq.Error); ok {
			if err.Code == "23505" {
				return s.ErrDuplicateKey
			}
		}
	}

	if err != nil {
		return fmt.Errorf("db error: %w", err)
	}
	return nil
}

func (s *Storage) Orders(login string) ([]Order, error) {
	var result []Order

	db, err := sqlx.Connect("postgres", s.DatabaseAddr)
	if err != nil {
		return result, fmt.Errorf("sql connect: %w", err)
	}
	defer db.Close()

	order := Order{}
	rows, err := db.Queryx("SELECT * FROM gom_orders")
	if err != nil {
		return result, fmt.Errorf("read rows: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.StructScan(&order)
		if err != nil {
			return result, fmt.Errorf("rows struct scan: %w", err)
		}
		result = append(result, order)
	}

	err = rows.Err()
	if err != nil {
		return result, fmt.Errorf("rows error: %w", err)
	}

	return result, nil
}

func (s *Storage) UserBalance(login string) (Balance, error) {
	balance := Balance{}

	db, err := sqlx.Connect("postgres", s.DatabaseAddr)
	if err != nil {
		return balance, fmt.Errorf("sql connect: %w", err)
	}
	defer db.Close()

	rows, err := db.Queryx("SELECT * FROM gom_balances WHERE login = $1", login)
	if err != nil {
		return balance, fmt.Errorf("read rows: %w", err)
	}
	defer rows.Close()

	rows.Next()
	err = rows.StructScan(&balance)
	if err != nil {
		return balance, fmt.Errorf("rows struct scan: %w", err)
	}

	err = rows.Err()
	if err != nil {
		return balance, fmt.Errorf("rows error: %w", err)
	}

	return balance, nil
}

func (s *Storage) Withdraw(login, orderNumber string, sum float64) (int, error) {
	db, err := sqlx.Connect("postgres", s.DatabaseAddr)
	if err != nil {
		return 0, fmt.Errorf("sql connect: %w", err)
	}
	defer db.Close()

	balance := Balance{}
	rows, err := db.Queryx("SELECT * FROM gom_balances WHERE login = $1", login)
	if err != nil {
		return 0, fmt.Errorf("read rows: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.StructScan(&balance); err != nil {
			return 0, fmt.Errorf("read rows: %w", err)
		}
		if balance.Current < sum {
			return 402, nil
		}
	}
	err = rows.Err()
	if err != nil {
		return 0, fmt.Errorf("rows error: %w", err)
	}

	_, err = db.Exec("UPDATE gom_balances SET current = $1, withdrawn = $2 WHERE login = $3", balance.Current-sum, balance.Withdrawn+sum, login)
	if err != nil {
		return 0, fmt.Errorf("db error: %w", err)
	}

	_, err = db.Exec("INSERT INTO gom_withdrawals VALUES ($1, $2, $3, $4, NOW())", uuidV4(), login, orderNumber, sum)
	if err != nil {
		return 0, fmt.Errorf("db error: %w", err)
	}

	return 0, nil
}

func (s *Storage) Withdrawals(login string) ([]Withdraw, error) {
	var result []Withdraw

	db, err := sqlx.Connect("postgres", s.DatabaseAddr)
	if err != nil {
		return result, fmt.Errorf("sql connect: %w", err)
	}
	defer db.Close()

	withdraw := Withdraw{}
	rows, err := db.Queryx("SELECT * FROM gom_withdrawals")
	if err != nil {
		return result, fmt.Errorf("read rows: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.StructScan(&withdraw)
		if err != nil {
			return result, fmt.Errorf("rows struct scan: %w", err)
		}
		result = append(result, withdraw)
	}

	err = rows.Err()
	if err != nil {
		return result, fmt.Errorf("rows error: %w", err)
	}

	return result, nil
}

func (s *Storage) OrdersProcessing() ([]Order, error) {
	var result []Order

	db, err := sqlx.Connect("postgres", s.DatabaseAddr)
	if err != nil {
		return result, fmt.Errorf("sql connect: %w", err)
	}
	defer db.Close()

	_, err = db.Exec("UPDATE gom_orders SET status = 'PROCESSING' WHERE status = 'NEW'")
	if err != nil {
		return result, fmt.Errorf("db update error: %w", err)
	}

	order := Order{}
	rows, err := db.Queryx("SELECT * FROM gom_orders WHERE status = 'PROCESSING'")
	if err != nil {
		return result, fmt.Errorf("read rows: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.StructScan(&order)
		if err != nil {
			return result, fmt.Errorf("rows struct scan: %w", err)
		}
		result = append(result, order)
	}

	err = rows.Err()
	if err != nil {
		return result, fmt.Errorf("rows error: %w", err)
	}

	return result, nil
}

func (s *Storage) SetOrderInvalid(orderNumber string) error {
	db, err := sqlx.Connect("postgres", s.DatabaseAddr)
	if err != nil {
		return fmt.Errorf("sql connect: %w", err)
	}
	defer db.Close()

	_, err = db.Exec("UPDATE gom_orders SET status = 'INVALID' WHERE number = $1", orderNumber)
	if err != nil {
		return fmt.Errorf("db update error: %w", err)
	}

	return nil
}

func (s *Storage) UserFromOrderNumber(orderNumber string) (string, error) {
	db, err := sqlx.Connect("postgres", s.DatabaseAddr)
	if err != nil {
		return "", fmt.Errorf("sql connect: %w", err)
	}
	defer db.Close()

	var user string
	rows, err := db.Queryx("SELECT login FROM gom_orders WHERE number = $1", orderNumber)
	if err != nil {
		return "", fmt.Errorf("read rows: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(&user); err != nil {
			return "", fmt.Errorf("read rows: %w", err)
		}
		return user, nil
	}
	err = rows.Err()
	if err != nil {
		return "", fmt.Errorf("rows error: %w", err)
	}
	return "", fmt.Errorf("unexpected error: %w", err)
}

func (s *Storage) SetOrderProcessed(orderNumber string, accrual float64) error {
	db, err := sqlx.Connect("postgres", s.DatabaseAddr)
	if err != nil {
		return fmt.Errorf("sql connect: %w", err)
	}
	defer db.Close()

	_, err = db.Exec("UPDATE gom_orders SET status = 'PROCESSED', accrual = $1 WHERE number = $2", accrual, orderNumber)
	if err != nil {
		return fmt.Errorf("db update error: %w", err)
	}

	login, err := s.UserFromOrderNumber(orderNumber)
	if err != nil {
		return fmt.Errorf("balance update error: %w", err)
	}

	balance := Balance{}
	rows, err := db.Queryx("SELECT * FROM gom_balances WHERE login = $1", login)
	if err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.StructScan(&balance); err != nil {
			return fmt.Errorf("read rows: %w", err)
		}
	}
	err = rows.Err()
	if err != nil {
		return fmt.Errorf("rows error: %w", err)
	}

	_, err = db.Exec("UPDATE gom_balances SET current = $1 WHERE login = $3", balance.Current+accrual, login)
	if err != nil {
		return fmt.Errorf("db error: %w", err)
	}

	return nil
}
