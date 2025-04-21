import os
import openpyxl
from openpyxl.styles import Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from datetime import datetime
from typing import List, Dict, Optional
import logging
import uuid

from logger import setup_logger

logger = setup_logger()

class ProposalGenerator:
    def __init__(self, output_dir: str = "proposals"):
        """
        Initialize ProposalGenerator with output directory.
        
        Args:
            output_dir: Directory to save generated proposals
        """
        self.output_dir = output_dir
        self._ensure_output_dir()
    
    def _ensure_output_dir(self):
        """Ensure output directory exists."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
    
    def generate(self, products: List[Dict], quantity: int = 10) -> str:
        """
        Generate commercial proposal in Excel format.
        
        Args:
            products: List of products to include in proposal
            quantity: Default quantity for each product (or stock if less)
            
        Returns:
            Path to generated Excel file
        """
        try:
            logger.info(f"Generating proposal with {len(products)} products")
            
            # Create a new workbook
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = "Коммерческое предложение"
            
            # Add headers
            self._add_headers(ws)
            
            # Add products
            self._add_products(ws, products, quantity)
            
            # Add total
            self._add_total(ws, len(products) + 3)  # +3 for header rows
            
            # Format worksheet
            self._format_worksheet(ws, len(products) + 4)  # +4 for header and total rows
            
            # Generate unique filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            uid = str(uuid.uuid4())[:8]
            filename = f"КП_{timestamp}_{uid}.xlsx"
            filepath = os.path.join(self.output_dir, filename)
            
            # Save workbook
            wb.save(filepath)
            logger.info(f"Proposal saved to: {filepath}")
            
            return filepath
            
        except Exception as e:
            logger.error(f"Error generating proposal: {str(e)}")
            raise
    
    def _add_headers(self, ws: openpyxl.worksheet.worksheet.Worksheet):
        """
        Add headers to the worksheet.
        
        Args:
            ws: Excel worksheet
        """
        # Add title
        ws.merge_cells('A1:E1')
        ws['A1'] = "Коммерческое предложение"
        ws['A1'].font = Font(size=14, bold=True)
        ws['A1'].alignment = Alignment(horizontal='center')
        
        # Add date
        ws.merge_cells('A2:E2')
        ws['A2'] = f"Дата: {datetime.now().strftime('%d.%m.%Y')}"
        ws['A2'].alignment = Alignment(horizontal='right')
        
        # Add column headers
        headers = ["№", "Наименование товара", "Цена (руб)", "Кол-во", "Сумма (руб)"]
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=3, column=col)
            cell.value = header
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
    
    def _add_products(self, ws: openpyxl.worksheet.worksheet.Worksheet, products: List[Dict], default_quantity: int):
        """
        Add products to the worksheet.
        
        Args:
            ws: Excel worksheet
            products: List of products
            default_quantity: Default quantity for each product
        """
        for i, product in enumerate(products, 1):
            # Determine quantity (default or stock if less)
            quantity = min(default_quantity, product.get('stock', default_quantity))
            
            # Calculate total for this product
            price = product.get('price', 0)
            total = price * quantity
            
            # Add data to worksheet
            ws.cell(row=i+3, column=1).value = i  # №
            ws.cell(row=i+3, column=2).value = product.get('name', '')  # Name
            ws.cell(row=i+3, column=3).value = price  # Price
            ws.cell(row=i+3, column=4).value = quantity  # Quantity
            ws.cell(row=i+3, column=5).value = total  # Total
            
            # Set alignment
            ws.cell(row=i+3, column=1).alignment = Alignment(horizontal='center')
            ws.cell(row=i+3, column=2).alignment = Alignment(vertical='center', wrap_text=True)
            ws.cell(row=i+3, column=3).alignment = Alignment(horizontal='right')
            ws.cell(row=i+3, column=4).alignment = Alignment(horizontal='center')
            ws.cell(row=i+3, column=5).alignment = Alignment(horizontal='right')
    
    def _add_total(self, ws: openpyxl.worksheet.worksheet.Worksheet, row: int):
        """
        Add total row to the worksheet.
        
        Args:
            ws: Excel worksheet
            row: Row number for the total
        """
        # Merge cells for "Итого"
        ws.merge_cells(f'A{row}:D{row}')
        ws[f'A{row}'] = "Итого:"
        ws[f'A{row}'].font = Font(bold=True)
        ws[f'A{row}'].alignment = Alignment(horizontal='right')
        
        # Add total formula
        last_row = row - 1
        ws[f'E{row}'] = f'=SUM(E4:E{last_row})'
        ws[f'E{row}'].font = Font(bold=True)
        ws[f'E{row}'].alignment = Alignment(horizontal='right')
    
    def _format_worksheet(self, ws: openpyxl.worksheet.worksheet.Worksheet, last_row: int):
        """
        Format worksheet with column widths, borders, etc.
        
        Args:
            ws: Excel worksheet
            last_row: Last row number in the worksheet
        """
        # Set column widths
        column_widths = {
            1: 5,   # №
            2: 50,  # Наименование товара
            3: 15,  # Цена (руб)
            4: 10,  # Кол-во
            5: 15,  # Сумма (руб)
        }
        
        for col, width in column_widths.items():
            ws.column_dimensions[get_column_letter(col)].width = width
        
        # Add borders to all cells in the table
        thin_border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        for row in range(3, last_row + 1):
            for col in range(1, 6):
                ws.cell(row=row, column=col).border = thin_border
        
        # Format numbers
        for row in range(4, last_row + 1):
            # Price column
            price_cell = ws.cell(row=row, column=3)
            price_cell.number_format = '#,##0.00'
            
            # Total column
            total_cell = ws.cell(row=row, column=5)
            total_cell.number_format = '#,##0.00' 