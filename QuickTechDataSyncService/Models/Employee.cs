using System;
using System.Collections.Generic;
namespace QuickTechDataSyncService.Models
{
    public class Employee
    {
        public int EmployeeId { get; set; }
        public string Username { get; set; } = string.Empty;
        public string PasswordHash { get; set; } = string.Empty;
        public string FirstName { get; set; } = string.Empty;
        public string LastName { get; set; } = string.Empty;
        public string Role { get; set; } = string.Empty;
        public bool IsActive { get; set; } = true;
        public DateTime CreatedAt { get; set; }
        public DateTime? LastLogin { get; set; }
        public decimal MonthlySalary { get; set; }
        public decimal CurrentBalance { get; set; }
        public virtual ICollection<EmployeeSalaryTransaction> SalaryTransactions { get; set; }
            = new List<EmployeeSalaryTransaction>();
    }

    public class EmployeeSalaryTransaction
    {
        public int Id { get; set; }
        public int EmployeeId { get; set; }
        public decimal Amount { get; set; }
        public string TransactionType { get; set; } = string.Empty;
        public DateTime TransactionDate { get; set; }
        public string? Notes { get; set; }

        public virtual Employee Employee { get; set; } = null!;
    }
}