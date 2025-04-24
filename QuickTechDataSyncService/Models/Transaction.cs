using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QuickTechDataSyncService.Models
{
    public enum TransactionType
    {
        Sale,
        Purchase,
        Adjustment
    }

    public enum TransactionStatus
    {
        Pending,
        Completed,
        Cancelled
    }

    public class Transaction
    {
        public int TransactionId { get; set; }
        public int? CustomerId { get; set; }
        public string CustomerName { get; set; } = string.Empty;
        public decimal TotalAmount { get; set; }
        public decimal PaidAmount { get; set; }
        public DateTime TransactionDate { get; set; }
        public TransactionType TransactionType { get; set; }
        public TransactionStatus Status { get; set; }
        public string PaymentMethod { get; set; } = string.Empty;
        public string CashierId { get; set; } = string.Empty;
        public string CashierName { get; set; } = string.Empty;
        public string CashierRole { get; set; } = string.Empty;

        // Navigation properties
        public virtual Customer? Customer { get; set; }
        public virtual ICollection<TransactionDetail> TransactionDetails { get; set; } = new List<TransactionDetail>();
    }
}