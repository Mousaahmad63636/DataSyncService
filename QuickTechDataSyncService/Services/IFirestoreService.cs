using QuickTechDataSyncService.Models;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QuickTechDataSyncService.Services
{
    public interface IFirestoreService
    {
        bool IsInitialized { get; }
        Task<bool> InitializeAsync();

        // Products
        Task<bool> SyncProductsAsync(IEnumerable<Product> products);
        Task<List<Product>> GetProductsAsync(DateTime? lastSyncTime = null);

        // Categories
        Task<bool> SyncCategoriesAsync(IEnumerable<Category> categories);

        // Customers
        Task<bool> SyncCustomersAsync(IEnumerable<Customer> customers);

        // Transactions
        Task<bool> SyncTransactionsAsync(IEnumerable<Transaction> transactions);

        // Business Settings
        Task<bool> SyncBusinessSettingsAsync(IEnumerable<BusinessSetting> settings);

        // Log sync operations
        Task LogSyncToFirestore(string deviceId, string entityType, bool success, int recordCount);
    }
}