package proc

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/inkpics/gophermart/internal/storage"
	"github.com/labstack/echo/v4"
)

type Proc struct {
	RunAddr      string
	DatabaseAddr string
	AccrualAddr  string
	Storage      *storage.Storage
	enc          string
}

func New(runAddr, databaseAddr, accrualAddr string) (*Proc, error) {
	s, err := storage.New(databaseAddr)
	if err != nil {
		err = fmt.Errorf("new storage: %w", err)
	}
	return &Proc{
		RunAddr:      runAddr,
		DatabaseAddr: databaseAddr,
		AccrualAddr:  accrualAddr,
		Storage:      s,
		enc:          "e0e10cbb-7713-43b4-9dc7-e198779e130c",
	}, err
}

type UserJSON struct {
	Login    string `json:"login"`
	Password string `json:"password"`
}

func encodePass(password string) string {
	h := md5.Sum([]byte(password))
	return hex.EncodeToString(h[:])
}

func (p *Proc) Register(c echo.Context) error {
	// StatusOK 200 — пользователь успешно зарегистрирован и аутентифицирован
	// StatusBadRequest 400 — неверный формат запроса
	// StatusConflict 409 — логин уже занят
	// StatusInternalServerError 500 — внутренняя ошибка сервера

	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	var u UserJSON
	err = json.Unmarshal(body, &u)
	if err != nil {
		return c.String(http.StatusBadRequest, "bad request")
	}

	err = p.Storage.UserRegister(u.Login, encodePass(u.Password))
	if errors.Is(err, p.Storage.ErrDuplicateKey) {
		return c.String(http.StatusConflict, "login is already in use")
	} else if err != nil {
		return c.String(http.StatusBadRequest, "bad request")
	}

	setLogin(c, u.Login, p.enc)

	return c.String(http.StatusOK, "user registered and authenticated successfully")
}

func (p *Proc) Login(c echo.Context) error {
	// StatusOK 200 — пользователь успешно аутентифицирован
	// StatusBadRequest 400 — неверный формат запроса
	// StatusUnauthorized 401 — неверная пара логин/пароль
	// StatusInternalServerError 500 — внутренняя ошибка сервера

	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	var u UserJSON
	err = json.Unmarshal(body, &u)
	if err != nil {
		return c.String(http.StatusBadRequest, "bad request")
	}

	err = p.Storage.UserLogin(u.Login, encodePass(u.Password))
	if err != nil {
		return c.String(http.StatusUnauthorized, "wrong credentials")
	}

	setLogin(c, u.Login, p.enc)

	return c.String(http.StatusOK, "user authenticated successfully")
}

func validateLuhn(number int) bool {
	return (number%10+checksumLuhn(number/10))%10 == 0
}

func checksumLuhn(number int) int {
	var luhn int

	for i := 0; number > 0; i++ {
		cur := number % 10

		if i%2 == 0 { // even
			cur = cur * 2
			if cur > 9 {
				cur = cur%10 + cur/10
			}
		}

		luhn += cur
		number = number / 10
	}
	return luhn % 10
}

func (p *Proc) SetOrders(c echo.Context) error {
	// StatusOK 200 — номер заказа уже был загружен этим пользователем
	// StatusAccepted 202 — новый номер заказа принят в обработку
	// StatusBadRequest 400 — неверный формат запроса
	// StatusUnauthorized 401 — пользователь не аутентифицирован
	// StatusConflict 409 — номер заказа уже был загружен другим пользователем
	// StatusUnprocessableEntity 422 — неверный формат номера заказа
	// StatusInternalServerError 500 — внутренняя ошибка сервера

	login, err := getLogin(c, p.enc)
	if err != nil {
		return c.String(http.StatusUnauthorized, "user authentication failed")
	}

	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	order := string(body)
	fmt.Println("new order text:", login, order)
	orderInt, err := strconv.Atoi(order)
	if err != nil {
		return c.String(http.StatusBadRequest, "bad request")
	}

	fmt.Println("new order:", login, order, validateLuhn(orderInt))

	if !validateLuhn(orderInt) {
		return c.String(http.StatusUnprocessableEntity, "incorrect order number")
	}

	registered, err := p.Storage.OrderRegistered(login, order)
	if err != nil {
		fmt.Println("new order OrderRegistered failed:", login, order)
		return c.String(http.StatusInternalServerError, "internal server error")
	}
	if registered == -1 {
		fmt.Println("new order already registered by another user:", login, order)
		return c.String(http.StatusConflict, "order registered by another user")
	} else if registered == 1 {
		fmt.Println("new order already registered:", login, order)
		return c.String(http.StatusOK, "order already registered")
	}

	err = p.Storage.OrderRegister(login, order)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}
	fmt.Println("new order registered:", login, order)
	return c.String(http.StatusAccepted, "order registered successfully")
}

type OrdersJSONItem struct {
	Number     string  `json:"number"`
	Status     string  `json:"status"`
	Accrual    float64 `json:"accrual"`
	UploadedAt string  `json:"uploaded_at"`
}

func (p *Proc) Orders(c echo.Context) error {
	// StatusOK 200 — успешная обработка запроса
	// StatusNoContent 204 — нет данных для ответа
	// StatusUnauthorized 401 — пользователь не авторизован
	// StatusInternalServerError 500 — внутренняя ошибка сервера

	login, err := getLogin(c, p.enc)
	if err != nil {
		return c.String(http.StatusUnauthorized, "user authentication failed")
	}

	orders, err := p.Storage.Orders(login)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}
	if orders == nil {
		return c.String(http.StatusNoContent, "user has no orders")
	}

	fmt.Println("orders:", login, orders)

	var arr []OrdersJSONItem
	for _, order := range orders {
		item := OrdersJSONItem{}
		item.Number = order.Number
		item.Status = order.Status
		item.Accrual = order.Accrual
		item.UploadedAt = order.UploadedAt
		arr = append(arr, item)
	}

	return c.JSON(http.StatusOK, arr)
}

type BalanceJSON struct {
	Current   float64 `json:"current"`
	Withdrawn float64 `json:"withdrawn"`
}

func (p *Proc) Balance(c echo.Context) error {
	// StatusOK 200 — успешная обработка запроса
	// StatusUnauthorized 401 — пользователь не авторизован
	// StatusInternalServerError 500 — внутренняя ошибка сервера

	login, err := getLogin(c, p.enc)
	if err != nil {
		return c.String(http.StatusUnauthorized, "user authentication failed")
	}

	balance, err := p.Storage.UserBalance(login)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	var result BalanceJSON
	result.Current = balance.Current
	result.Withdrawn = balance.Withdrawn

	return c.JSON(http.StatusOK, result)
}

type WithdrawJSON struct {
	Order string  `json:"order"`
	Sum   float64 `json:"sum"`
}

func (p *Proc) Withdraw(c echo.Context) error {
	// StatusOK 200 — успешная обработка запроса
	// StatusUnauthorized 401 — пользователь не авторизован
	// StatusPaymentRequired 402 — на счету недостаточно средств
	// StatusUnprocessableEntity 422 — неверный номер заказа
	// StatusInternalServerError 500 — внутренняя ошибка сервера

	login, err := getLogin(c, p.enc)
	if err != nil {
		return c.String(http.StatusUnauthorized, "user authentication failed")
	}

	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	var w WithdrawJSON
	err = json.Unmarshal(body, &w)
	if err != nil {
		return c.String(http.StatusBadRequest, "bad request")
	}

	orderInt, err := strconv.Atoi(w.Order)
	if err != nil {
		return c.String(http.StatusBadRequest, "bad request")
	}

	if !validateLuhn(orderInt) {
		return c.String(http.StatusUnprocessableEntity, "incorrect order number")
	}

	withdraw, err := p.Storage.Withdraw(login, w.Order, w.Sum)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}
	if withdraw == 402 {
		return c.String(http.StatusPaymentRequired, "not enough cash")
	}

	return c.String(http.StatusOK, "successfull withdraw")
}

type WithdrawalsJSONItem struct {
	OrderNumber string  `json:"order"`
	Sum         float64 `json:"sum"`
	ProcessedAt string  `json:"processed_at"`
}

func (p *Proc) Withdrawals(c echo.Context) error {
	// 200 — успешная обработка запроса
	// 204 — нет ни одного списания
	// 401 — пользователь не авторизован
	// 500 — внутренняя ошибка сервера

	login, err := getLogin(c, p.enc)
	if err != nil {
		return c.String(http.StatusUnauthorized, "user authentication failed")
	}

	withdrawals, err := p.Storage.Withdrawals(login)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}
	if withdrawals == nil {
		return c.String(http.StatusNoContent, "user has no withdrawals")
	}

	var arr []WithdrawalsJSONItem
	for _, withdraw := range withdrawals {
		item := WithdrawalsJSONItem{}
		item.OrderNumber = withdraw.OrderNumber
		item.Sum = withdraw.Sum
		item.ProcessedAt = withdraw.ProcessedAt
		arr = append(arr, item)
	}

	return c.JSON(http.StatusOK, arr)
}

func signition(person, enc string) string {
	hm := hmac.New(sha256.New, []byte(enc))
	hm.Write([]byte(person))
	result := hm.Sum(nil)
	return hex.EncodeToString(result)[:16]
}

func cookie(c echo.Context, name, val string) (string, error) {
	coo := new(http.Cookie)

	if val == "" {
		coo, err := c.Cookie(name)
		if err != nil {
			return "", err
		}
		return coo.Value, nil
	}

	coo.Name = name
	coo.Value = val
	c.SetCookie(coo)
	return "", nil
}

func setLogin(c echo.Context, login, enc string) {
	token, err := cookie(c, "token", "")
	if err == nil && token == signition(login, enc) {
		return
	}

	cookie(c, "person", login)
	cookie(c, "token", signition(login, enc))
}

func getLogin(c echo.Context, enc string) (string, error) {
	login, err1 := cookie(c, "person", "")
	token, err2 := cookie(c, "token", "")
	if err1 == nil && err2 == nil && token == signition(login, enc) {
		return login, nil
	}

	return login, fmt.Errorf("authentification check failed")
}

func (p *Proc) AccrualLoop() {
	for {
		p.UpdateAccrual()
		time.Sleep(3 * time.Second)
	}
}

func (p *Proc) UpdateAccrual() error {
	orders, err := p.Storage.OrdersProcessing()
	if err != nil {
		return fmt.Errorf("update accrual error: %w", err)
	}

	for _, order := range orders {
		status, accrual, err := p.Accrual(order.Number)
		if err != nil {
			return fmt.Errorf("update accrual order error: %w", err)
		}
		if status == "INVALID" {
			err = p.Storage.SetOrderInvalid(order.Number)
			if err != nil {
				return fmt.Errorf("set order invalid error: %w", err)
			}
		} else if status == "PROCESSED" {
			err = p.Storage.SetOrderProcessed(order.Number, accrual)
			if err != nil {
				return fmt.Errorf("set order processed error: %w", err)
			}
		}
	}

	return nil
}

type AccrualJSON struct {
	OrderNumber string  `json:"order"`
	Status      string  `json:"status"`
	Accrual     float64 `json:"accrual"`
}

func (p *Proc) Accrual(orderNumber string) (string, float64, error) {
	resp, err := http.Get(p.AccrualAddr + "/api/orders/" + orderNumber)
	if err != nil {
		return "", 0, fmt.Errorf("accrual error: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("accrual error: %w", err)
	}

	var a AccrualJSON
	err = json.Unmarshal(body, &a)
	if err != nil {
		return "", 0, fmt.Errorf("accrual error: %w", err)
	}

	return a.Status, a.Accrual, nil
}
