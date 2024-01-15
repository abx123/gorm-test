package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Withdrawal struct {
	gorm.Model
	WithdrawalID       uuid.UUID `gorm:"type:uuid; default:gen_random_uuid(); not null"`
	BlockchainID       uuid.UUID `gorm:"type:uuid; not null"`
	AddressSourceID    uuid.UUID `gorm:"type:uuid; not null"`
	AddressDestination string    `gorm:"type:varchar(256); not null"`
	Amount             string    `gorm:"type:varchar(128); not null"`
	AssetID            uuid.UUID `gorm:"type:uuid; not null"`
	Mquorum            int       `gorm:"type:int; not null"`
	Ledgers            []Ledger
}

type Ledger struct {
	gorm.Model
	WithdrawalID uint    `gorm:"type:integer; not null"`
	Signatures   *string `gorm:"type:varchar(512)"`
	Broadcasted  string  `gorm:"type:varchar(64); not null"`
	TxHash       *string `gorm:"type:varchar(256)"`
	TxPayload    *string `gorm:"type:varchar(512)"`
	Remarks      *string `gorm:"type:varchar(256)"`
}

func main() {
	sqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s database=%s sslmode=disable", "localhost", 5432, "postgres", "password", "test")
	db, err := gorm.Open(postgres.Open(sqlInfo), &gorm.Config{})
	if err != nil {
		panic("Failed to connect to database")
	}

	db.AutoMigrate(&Withdrawal{}, &Ledger{})
	sig := "Signature1"
	w, err := createWithdrawal(db, &sig, "0x123", "100", "0", context.Background())
	if err != nil {
		fmt.Println("ERROR", err.Error())
	}
	fmt.Println("Withdrawal", w)
	remarks := "Remarks"
	tx_hash := "txHASH"
	ww, err := updateWithdrawal(db, &remarks, nil, &tx_hash, []string{"SIG2", "SIG3", "SIG4"}, w.WithdrawalID.String(), "NO", w.UpdatedAt, context.Background())

	if err != nil {
		fmt.Println("ERROR", err.Error())
	}

	fmt.Println("Withdrawal2", ww)

}

func createWithdrawal(db *gorm.DB, Signatures *string, addressDestination, amount, broadcasted string, ctx context.Context) (*Withdrawal, error) {
	ledger := Ledger{
		Broadcasted: "NO",
		Signatures:  Signatures,
	}
	withdrawal := &Withdrawal{
		BlockchainID:       uuid.New(),
		AddressSourceID:    uuid.New(),
		AddressDestination: addressDestination,
		Amount:             amount,
		AssetID:            uuid.New(),
		Mquorum:            1,
		Ledgers:            []Ledger{ledger},
	}
	result := db.WithContext(ctx).Clauses(clause.Returning{}).Create(withdrawal)
	if result.Error != nil {
		return nil, result.Error
	}
	return withdrawal, nil

}

func updateWithdrawal(db *gorm.DB, remarks, txPayload, txHash *string, Signatures []string, withdrawalID, broadcasted string, updatedAt time.Time, ctx context.Context) (*Withdrawal, error) {
	withdrawal := &Withdrawal{}
	if r := db.WithContext(ctx).Where("withdrawal_id = ? and updated_at = ?", withdrawalID, updatedAt).Preload("Ledgers").First(withdrawal); r.Error != nil || r.RowsAffected == 0 {
		if r.RowsAffected == 0 {
			return nil, errors.New("Record is stale, please refresh and try again")
		}
		return nil, r.Error
	}
	var val *string

	if withdrawal.Ledgers[len(withdrawal.Ledgers)-1].Signatures != nil {
		val = withdrawal.Ledgers[len(withdrawal.Ledgers)-1].Signatures
	}

	signatureString := strings.Join(Signatures, ",")

	if val != nil {
		signatureString = *val + "," + signatureString
	}

	newLedger := Ledger{
		WithdrawalID: withdrawal.ID,
		Broadcasted:  broadcasted,
		Signatures:   &signatureString,
	}
	if txHash != nil {
		newLedger.TxHash = txHash
	}
	if txPayload != nil {
		newLedger.TxPayload = txPayload
	}
	if remarks != nil {
		newLedger.Remarks = remarks
	}

	withdrawal.Ledgers = append(withdrawal.Ledgers, newLedger)

	result := db.WithContext(ctx).Clauses(clause.Returning{}).Save(withdrawal)
	if result.Error != nil {
		return nil, result.Error
	}
	return withdrawal, nil

}
