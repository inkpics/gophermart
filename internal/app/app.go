package app

import (
	"fmt"

	"github.com/inkpics/gophermart/internal/proc"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func Start(runAddr, databaseAddr, accrualAddr string) error {
	p, err := proc.New(runAddr, databaseAddr, accrualAddr)
	if err != nil {
		return fmt.Errorf("handler: %w", err)
	}

	go p.AccrualLoop()

	e := echo.New()
	e.Use(middleware.Gzip())
	e.Use(middleware.Decompress())

	// регистрация пользователя
	e.POST("/api/user/register", p.Register)

	// аутентификация пользователя
	e.POST("/api/user/login", p.Login)

	// загрузка пользователем номера заказа для расчёта
	e.POST("/api/user/orders", p.SetOrders, p.MiddlewareAuth)

	// получение списка загруженных пользователем номеров заказов, статусов их обработки и информации о начислениях
	e.GET("/api/user/orders", p.Orders, p.MiddlewareAuth)

	// получение текущего баланса счёта баллов лояльности пользователя
	e.GET("/api/user/balance", p.Balance, p.MiddlewareAuth)

	// запрос на списание баллов с накопительного счёта в счёт оплаты нового заказа
	e.POST("/api/user/balance/withdraw", p.Withdraw, p.MiddlewareAuth)

	// получение информации о выводе средств с накопительного счёта пользователя
	e.GET("/api/user/withdrawals", p.Withdrawals, p.MiddlewareAuth)

	e.Logger.Fatal(e.Start(runAddr))

	return nil
}
