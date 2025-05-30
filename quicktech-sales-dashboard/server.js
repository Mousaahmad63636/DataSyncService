const express = require('express');
const cors = require('cors');
const path = require('path');
const { connectToDatabase } = require('./db');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/sales', async (req, res) => {
  try {
    const { db } = await connectToDatabase();
    const { date, period, employee } = req.query;
    
    let startDate, endDate;
    
    if (date) {
      if (period === 'week') {
        startDate = new Date(date);
        endDate = new Date(date);
        endDate.setDate(endDate.getDate() + 6);
        endDate.setHours(23, 59, 59, 999);
      } else if (period === 'month') {
        startDate = new Date(date);
        endDate = new Date(date);
        endDate.setMonth(endDate.getMonth() + 1);
        endDate.setDate(0);
        endDate.setHours(23, 59, 59, 999);
      } else {
        startDate = new Date(date);
        startDate.setHours(0, 0, 0, 0);
        
        endDate = new Date(date);
        endDate.setHours(23, 59, 59, 999);
      }
    } else {
      startDate = new Date();
      startDate.setHours(0, 0, 0, 0);
      
      endDate = new Date();
      endDate.setHours(23, 59, 59, 999);
    }
    
    // Build the query with date and transaction type
    let query = {
      transactionDate: { 
        $gte: startDate, 
        $lte: endDate 
      },
      transactionType: "Sale"
    };
    
    // Add employee filter if provided
    if (employee && employee !== 'all') {
      query.cashierId = employee;
    }
    
    const transactions = await db.collection('transactions')
      .find(query)
      .sort({ transactionDate: -1 })
      .limit(100)
      .toArray();
      
    // Create date strings to handle timezone properly for expense queries
    const startDateStr = startDate.toISOString().split('T')[0];
    const endDateStr = endDate.toISOString().split('T')[0];
    
    // Query for expenses using multiple date formats to ensure we catch all records
    const expenses = await db.collection('expenses')
      .find({
        $or: [
          // Option 1: If date is stored as a Date object
          {
            date: { 
              $gte: startDate, 
              $lte: endDate 
            }
          },
          // Option 2: If date is stored as a string (ISO format)
          {
            date: { 
              $gte: startDateStr,
              $lte: endDateStr + 'T23:59:59.999Z'
            }
          },
          // Option 3: Try with createdAt field if date field isn't working
          {
            createdAt: {
              $gte: startDate, 
              $lte: endDate
            }
          }
        ]
      })
      .toArray();

    // Process transactions to ensure amount is a valid number
    const processedTransactions = transactions.map(tx => {
      let amount = 0;
      if (tx.totalAmount) {
        const amountStr = tx.totalAmount.toString();
        amount = parseFloat(amountStr);
        if (isNaN(amount)) amount = 0;
      }
      
      return {
        ...tx,
        totalAmount: amount
      };
    });
    
    const totalSales = processedTransactions.reduce((sum, tx) => sum + tx.totalAmount, 0);
    const totalTransactions = processedTransactions.length;
    
    const totalExpense = expenses.reduce((sum, expense) => {
      let amount = 0;
      if (expense.amount) {
        const amountStr = expense.amount.toString();
        amount = parseFloat(amountStr);
        if (isNaN(amount)) amount = 0;
      }
      return sum + amount;
    }, 0);
    
    res.status(200).json({
      transactions: processedTransactions,
      summary: {
        totalSales,
        totalTransactions,
        totalExpense,
        date: startDate.toISOString(),
        startDate: startDate.toISOString(),
        endDate: endDate.toISOString(),
        period: period || 'day'
      }
    });
  } catch (error) {
    console.error('Error fetching sales data:', error);
    res.status(500).json({ error: 'فشل في جلب بيانات المبيعات' });
  }
});

// Add a new endpoint to get all employees
app.get('/api/employees', async (req, res) => {
  try {
    const { db } = await connectToDatabase();
    
    // Get unique cashier IDs and names from transactions
    const employees = await db.collection('transactions')
      .aggregate([
        { 
          $match: { 
            cashierId: { $exists: true, $ne: "" },
            cashierName: { $exists: true, $ne: "" }
          }
        },
        {
          $group: {
            _id: "$cashierId",
            name: { $first: "$cashierName" },
            role: { $first: "$cashierRole" }
          }
        },
        { $sort: { name: 1 } }
      ])
      .toArray();
    
    res.status(200).json(employees);
  } catch (error) {
    console.error('Error fetching employees:', error);
    res.status(500).json({ error: 'فشل في جلب بيانات الموظفين' });
  }
});

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});

module.exports = app;