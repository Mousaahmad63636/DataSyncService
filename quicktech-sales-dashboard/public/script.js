document.addEventListener('DOMContentLoaded', function() {
    const dateInput = document.getElementById('date');
    const periodButtons = document.querySelectorAll('.period-filter button');
    const employeeSelect = document.getElementById('employee-select');
    const totalSalesEl = document.querySelector('#total-sales .value');
    const totalTransactionsEl = document.querySelector('#total-transactions .value');
    const totalExpenseEl = document.querySelector('#total-expense .value');
    const totalProfitEl = document.querySelector('#total-profit .value');
    const loadingEl = document.getElementById('loading');
    const errorEl = document.getElementById('error');
    const noDataEl = document.getElementById('no-data');
    const transactionsTable = document.getElementById('transactions-table');
    const transactionsBody = document.getElementById('transactions-body');
    const transactionCards = document.getElementById('transaction-cards');
    
    let currentPeriod = 'today';
    let selectedEmployee = 'all';
    
    const today = new Date();
    const formattedDate = formatDate(today);
    dateInput.value = formattedDate;
    
    fetchEmployees().then(() => {
        fetchSalesData(formattedDate, currentPeriod, selectedEmployee);
    });
    
    dateInput.addEventListener('change', function() {
        fetchSalesData(this.value, 'custom', selectedEmployee);
        
        periodButtons.forEach(button => {
            button.classList.remove('active');
        });
    });
    
    employeeSelect.addEventListener('change', function() {
        selectedEmployee = this.value;
        fetchSalesData(dateInput.value, currentPeriod, selectedEmployee);
    });
    
    periodButtons.forEach(button => {
        button.addEventListener('click', function() {
            const period = this.getAttribute('data-period');
            currentPeriod = period;
            
            periodButtons.forEach(btn => {
                btn.classList.remove('active');
            });
            this.classList.add('active');
            
            let date;
            switch(period) {
                case 'today':
                    date = formatDate(new Date());
                    break;
                case 'week':
                    date = getStartOfWeek();
                    break;
                case 'month':
                    date = getStartOfMonth();
                    break;
                default:
                    date = formatDate(new Date());
            }
            
            dateInput.value = date;
            fetchSalesData(date, period, selectedEmployee);
        });
    });
    
    async function fetchEmployees() {
        try {
            const response = await fetch('/api/employees');
            if (!response.ok) {
                throw new Error('فشل في جلب بيانات الموظفين');
            }
            
            const employees = await response.json();
            
            while (employeeSelect.options.length > 1) {
                employeeSelect.remove(1);
            }
            
            employees.forEach(emp => {
                const option = document.createElement('option');
                option.value = emp._id;
                option.textContent = emp.name;
                employeeSelect.appendChild(option);
            });
            
        } catch (err) {
            console.error('Error fetching employees:', err);
        }
    }
    
    function fetchSalesData(date, period, employeeId) {
        showLoading();
        
        let url = `/api/sales?date=${date}`;
        if(period) {
            url += `&period=${period}`;
        }
        if(employeeId && employeeId !== 'all') {
            url += `&employee=${employeeId}`;
        }
        
        fetch(url)
            .then(response => {
                if (!response.ok) {
                    throw new Error('فشل في جلب البيانات');
                }
                return response.json();
            })
            .then(data => {
                hideLoading();
                data.summary.startDate = new Date(data.summary.startDate);
                data.summary.endDate = new Date(data.summary.endDate);
                displayData(data);
            })
            .catch(err => {
                hideLoading();
                showError('خطأ في تحميل بيانات المبيعات. يرجى المحاولة لاحقاً.');
                console.error(err);
            });
    }
    
    function displayData(data) {
        const { transactions, summary } = data;
        
        totalSalesEl.textContent = `$${summary.totalSales.toFixed(2)}`;
        totalTransactionsEl.textContent = summary.totalTransactions;
        totalExpenseEl.textContent = `$${summary.totalExpense.toFixed(2)}`;
        
        const profit = summary.totalSales - summary.totalExpense;
        totalProfitEl.textContent = `$${profit.toFixed(2)}`;

        // Clear previous content
        transactionsBody.innerHTML = '';
        transactionCards.innerHTML = '';
        
        if (transactions.length === 0) {
            noDataEl.style.display = 'block';
            transactionsTable.style.display = 'none';
            transactionCards.style.display = 'none';
        } else {
            noDataEl.style.display = 'none';
            transactionCards.style.display = 'flex';
            
            transactions.forEach(tx => {
                // Process transaction data
                const txDate = new Date(tx.transactionDate);
                const formattedDate = `${txDate.getDate()}/${txDate.getMonth() + 1}/${txDate.getFullYear()} ${txDate.getHours().toString().padStart(2, '0')}:${txDate.getMinutes().toString().padStart(2, '0')}`;
                
                const rawAmount = tx.totalAmount;
                const amount = !isNaN(parseFloat(rawAmount)) ? parseFloat(rawAmount) : 0;
                
                // Create card for mobile view
                const card = document.createElement('div');
                card.className = 'transaction-card';
                
                card.innerHTML = `
                    <div class="transaction-card-header">
                        <div class="transaction-id">#${tx.transactionId}</div>
                        <div class="transaction-amount">$${amount.toFixed(2)}</div>
                    </div>
                    <div class="transaction-detail">
                        <div class="detail-label">العميل:</div>
                        <div class="detail-value">${tx.customerName || 'عميل عابر'}</div>
                    </div>
                    <div class="transaction-detail">
                        <div class="detail-label">الموظف:</div>
                        <div class="detail-value">${tx.cashierName || 'غير محدد'}</div>
                    </div>
                    <div class="transaction-detail">
                        <div class="detail-label">التاريخ:</div>
                        <div class="detail-value">${formattedDate}</div>
                    </div>
                `;
                
                transactionCards.appendChild(card);
                
                // Also create row for table (for larger screens)
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${tx.transactionId}</td>
                    <td>${tx.customerName || 'عميل عابر'}</td>
                    <td>${tx.cashierName || 'غير محدد'}</td>
                    <td>$${amount.toFixed(2)}</td>
                `;
                
                transactionsBody.appendChild(row);
            });
        }
    }
    
    function showLoading() {
        loadingEl.style.display = 'flex';
        errorEl.style.display = 'none';
        noDataEl.style.display = 'none';
        transactionsTable.style.display = 'none';
        transactionCards.style.display = 'none';
    }
    
    function hideLoading() {
        loadingEl.style.display = 'none';
    }
    
    function showError(message) {
        errorEl.textContent = message;
        errorEl.style.display = 'block';
        noDataEl.style.display = 'none';
        transactionsTable.style.display = 'none';
        transactionCards.style.display = 'none';
    }
    
    function formatDate(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    }
    
    function getStartOfWeek() {
        const now = new Date();
        const dayOfWeek = now.getDay();
        const diff = now.getDate() - dayOfWeek + (dayOfWeek === 0 ? -6 : 1);
        const startOfWeek = new Date(now.setDate(diff));
        return formatDate(startOfWeek);
    }
    
    function getStartOfMonth() {
        const now = new Date();
        const startOfMonth = new Date(now.getFullYear(), now.getMonth(), 1);
        return formatDate(startOfMonth);
    }
});