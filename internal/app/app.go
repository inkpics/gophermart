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

	e := echo.New()
	e.Use(middleware.Gzip())
	e.Use(middleware.Decompress())

	e.POST("/api/user/register", p.Register)         // регистрация пользователя
	e.POST("/api/user/login", p.Login)               // аутентификация пользователя
	e.POST("/api/user/orders", p.SetOrders)          // загрузка пользователем номера заказа для расчёта
	e.GET("/api/user/orders", p.Orders)              // получение списка загруженных пользователем номеров заказов, статусов их обработки и информации о начислениях
	e.GET("/api/user/balance", p.Balance)            // получение текущего баланса счёта баллов лояльности пользователя
	e.POST("/api/user/balance/withdraw", p.Withdraw) // запрос на списание баллов с накопительного счёта в счёт оплаты нового заказа
	e.GET("/api/user/withdrawals", p.Withdrawals)    // получение информации о выводе средств с накопительного счёта пользователя

	e.Logger.Fatal(e.Start(runAddr))

	return nil
}
