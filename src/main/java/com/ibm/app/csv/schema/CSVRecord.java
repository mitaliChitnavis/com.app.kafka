package com.ibm.app.csv.schema;

import com.opencsv.bean.CsvBindByPosition;

public class CSVRecord {
	public CSVRecord() {
		// TODO Auto-generated constructor stub
	}
	@CsvBindByPosition(position=0)
	private String invoiceNo;
	@CsvBindByPosition(position=1)
	private String stockCode;
	@CsvBindByPosition(position=2)
	private String description;
	@CsvBindByPosition(position=3)
	private String quantity;
	@CsvBindByPosition(position=4)
	private String invoiceDate;
	@CsvBindByPosition(position=5)
	private String unitPrice;
	@CsvBindByPosition(position=6)
	private String customerID;
	@CsvBindByPosition(position=7)
	private String country;
	
	public CSVRecord(String invoiceNo, String stockCode, String description, String quantity, String invoiceDate,
			String unitPrice, String customerID, String country) {
		super();
		this.invoiceNo = invoiceNo;
		this.stockCode = stockCode;
		this.description = description;
		this.quantity = quantity;
		this.invoiceDate = invoiceDate;
		this.unitPrice = unitPrice;
		this.customerID = customerID;
		this.country = country;
	}
	
	public String getInvoiceNo() {
		return invoiceNo;
	}
	public String getStockCode() {
		return stockCode;
	}
	public String getDescription() {
		return description;
	}
	public String getQuantity() {
		return quantity;
	}
	public String getInvoiceDate() {
		return invoiceDate;
	}
	public String getUnitPrice() {
		return unitPrice;
	}
	public String getCustomerID() {
		return customerID;
	}
	public String getCountry() {
		return country;
	}
	public void setInvoiceNo(String invoiceNo) {
		this.invoiceNo = invoiceNo;
	}
	public void setStockCode(String stockCode) {
		this.stockCode = stockCode;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public void setQuantity(String quantity) {
		this.quantity = quantity;
	}
	public void setInvoiceDate(String invoiceDate) {
		this.invoiceDate = invoiceDate;
	}
	public void setUnitPrice(String unitPrice) {
		this.unitPrice = unitPrice;
	}
	public void setCustomerID(String customerID) {
		this.customerID = customerID;
	}
	public void setCountry(String country) {
		this.country = country;
	}


}
