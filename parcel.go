package main

import (
	"database/sql"
	"fmt"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p

	res, err := s.db.Exec("INSERT INTO parcel (client, status, address, created_at) VALUES (?, ?, ?, ?)",
		p.Client, p.Status, p.Address, p.CreatedAt,
	)
	if err != nil {
		return 0, fmt.Errorf("ошибка при добавлении посылки: %v", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("ошибка при получении ID: %v", err)
	}

	// верните идентификатор последней добавленной записи
	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка

	row := s.db.QueryRow(
		"SELECT number, client, status, address, created_at FROM parcel WHERE number = ?",
		number,
	)

	// заполните объект Parcel данными из таблицы
	p := Parcel{}

	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return p, fmt.Errorf("посылка с номером %d не найдена", number)
		}
		return p, fmt.Errorf("ошибка при чтении посылки: %v", err)
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// реализуйте чтение строк из таблицы parcel по заданному client
	// здесь из таблицы может вернуться несколько строк

	rows, err := s.db.Query(
		"SELECT number, client, status, address, created_at FROM parcel WHERE client = ?",
		client,
	)
	if err != nil {
		return nil, fmt.Errorf("ошибка при запросе посылок клиента: %v", err)
	}
	defer rows.Close()

	// заполните срез Parcel данными из таблицы
	var res []Parcel

	for rows.Next() {
		p := Parcel{}
		err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("ошибка при сканировании посылки: %v", err)
		}
		res = append(res, p)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при обработке результатов: %v", err)
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	// реализуйте обновление статуса в таблице parcel

	_, err := s.db.Exec(
		"UPDATE parcel SET status = ? WHERE number = ?",
		status, number,
	)
	if err != nil {
		return fmt.Errorf("ошибка при обновлении статуса: %v", err)
	}

	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered

	p, err := s.Get(number)
	if err != nil {
		return fmt.Errorf("ошибка при проверке статуса: %v", err)
	}

	if p.Status != ParcelStatusRegistered {
		return fmt.Errorf("нельзя изменить адрес: статус посылки должен быть '%s', а не '%s'",
			ParcelStatusRegistered, p.Status)
	}

	_, err = s.db.Exec(
		"UPDATE parcel SET address = ? WHERE number = ?",
		address, number,
	)
	if err != nil {
		return fmt.Errorf("ошибка при обновлении адреса: %v", err)
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered

	p, err := s.Get(number)
	if err != nil {
		return fmt.Errorf("ошибка при проверке статуса: %v", err)
	}

	if p.Status != ParcelStatusRegistered {
		return fmt.Errorf("нельзя удалить посылку: статус должен быть '%s', а не '%s'",
			ParcelStatusRegistered, p.Status)
	}

	_, err = s.db.Exec(
		"DELETE FROM parcel WHERE number = ?",
		number,
	)
	if err != nil {
		return fmt.Errorf("ошибка при удалении посылки: %v", err)
	}

	return nil
}
