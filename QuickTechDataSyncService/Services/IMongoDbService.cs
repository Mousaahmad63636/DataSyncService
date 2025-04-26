using QuickTechDataSyncService.Models;
using MongoDB.Driver;
using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QuickTechDataSyncService.Services
{
    public interface IMongoDbService
    {
        bool IsInitialized { get; }
        Task<bool> InitializeAsync();
        Task<bool> SyncProductsAsync(IEnumerable<Product> products);
        Task<List<Product>> GetProductsAsync(DateTime? lastSyncTime = null);
        Task<bool> SyncCategoriesAsync(IEnumerable<Category> categories);
        Task<bool> SyncCustomersAsync(IEnumerable<Customer> customers);
        Task<bool> SyncTransactionsAsync(IEnumerable<Transaction> transactions);
        Task<bool> SyncBusinessSettingsAsync(IEnumerable<BusinessSetting> settings);
        Task LogSyncActivityAsync(string deviceId, string entityType, bool success, int recordCount);
        Task<bool> SyncExpensesAsync(IEnumerable<Expense> expenses);
        Task<bool> SyncEmployeesAsync(IEnumerable<Employee> employees);

        // New method to expose the MongoDB database for direct access when needed
        IMongoDatabase GetMongoDatabase();
    }
}