using System;
namespace QuickTechDataSyncService.Models
{
    public class Expense
    {
        public int ExpenseId { get; set; }
        public string Reason { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public DateTime Date { get; set; }
        public string? Notes { get; set; }
        public string Category { get; set; } = string.Empty;
        public bool IsRecurring { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime? UpdatedAt { get; set; }
    }
}