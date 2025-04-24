using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using QuickTechDataSyncService.Data;
using QuickTechDataSyncService.Models;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace QuickTechDataSyncService.Services
{
    public class FirebaseSyncService : IFirebaseSyncService
    {
        private readonly ApplicationDbContext _dbContext;
        private readonly IFirestoreService _firestoreService;
        private readonly ILogger<FirebaseSyncService> _logger;

        public FirebaseSyncService(
            ApplicationDbContext dbContext,
            IFirestoreService firestoreService,
            ILogger<FirebaseSyncService> logger)
        {
            _dbContext = dbContext;
            _firestoreService = firestoreService;
            _logger = logger;
        }

        public async Task<bool> InitializeFirebaseAsync()
        {
            return await _firestoreService.InitializeAsync();
        }

        public bool IsFirebaseInitialized => _firestoreService.IsInitialized;

        public async Task<SyncResult> SyncAllDataToFirebaseAsync(string deviceId)
        {
            var result = new SyncResult
            {
                StartTime = DateTime.UtcNow,
                DeviceId = deviceId
            };

            try
            {
                if (!_firestoreService.IsInitialized)
                {
                    var initialized = await _firestoreService.InitializeAsync();
                    if (!initialized)
                    {
                        result.Success = false;
                        result.ErrorMessage = "Failed to initialize Firestore";
                        return result;
                    }
                }

                // Sync products and count records
                var products = await _dbContext.Products.Include(p => p.Category).ToListAsync();
                var productsSuccess = await _firestoreService.SyncProductsAsync(products);
                result.RecordCounts.Add("Products", products.Count);

                // Sync categories
                var categories = await _dbContext.Categories.ToListAsync();
                var categoriesSuccess = await _firestoreService.SyncCategoriesAsync(categories);
                result.RecordCounts.Add("Categories", categories.Count);

                // Sync customers
                var customers = await _dbContext.Customers.ToListAsync();
                var customersSuccess = await _firestoreService.SyncCustomersAsync(customers);
                result.RecordCounts.Add("Customers", customers.Count);

                // Sync business settings
                var settings = await _dbContext.BusinessSettings.ToListAsync();
                var settingsSuccess = await _firestoreService.SyncBusinessSettingsAsync(settings);
                result.RecordCounts.Add("BusinessSettings", settings.Count);

                // Sync transactions (may be large, so limit to recent ones)
                var recentTransactions = await _dbContext.Transactions
                    .Include(t => t.TransactionDetails)
                    .OrderByDescending(t => t.TransactionDate)
                    .Take(1000)  // Consider syncing in batches for production
                    .ToListAsync();

                var transactionsSuccess = await _firestoreService.SyncTransactionsAsync(recentTransactions);
                result.RecordCounts.Add("Transactions", recentTransactions.Count);

                // Overall success based on all operations
                result.Success = productsSuccess && categoriesSuccess && customersSuccess &&
                                settingsSuccess && transactionsSuccess;

                // Log the sync operation both locally and in Firestore
                await LogSyncActivity(deviceId, "All", result.Success,
                    result.RecordCounts.Values.Sum());

                result.EndTime = DateTime.UtcNow;
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during full Firebase sync");
                result.Success = false;
                result.ErrorMessage = ex.Message;
                result.EndTime = DateTime.UtcNow;
                return result;
            }
        }

        public async Task<SyncResult> SyncEntityToFirebaseAsync(string deviceId, string entityType)
        {
            var result = new SyncResult
            {
                StartTime = DateTime.UtcNow,
                DeviceId = deviceId,
                EntityType = entityType
            };

            try
            {
                if (!_firestoreService.IsInitialized)
                {
                    var initialized = await _firestoreService.InitializeAsync();
                    if (!initialized)
                    {
                        result.Success = false;
                        result.ErrorMessage = "Failed to initialize Firestore";
                        return result;
                    }
                }

                int recordCount = 0;
                bool success = false;

                switch (entityType.ToLower())
                {
                    case "products":
                        var products = await _dbContext.Products.Include(p => p.Category).ToListAsync();
                        success = await _firestoreService.SyncProductsAsync(products);
                        recordCount = products.Count;
                        break;

                    case "categories":
                        var categories = await _dbContext.Categories.ToListAsync();
                        success = await _firestoreService.SyncCategoriesAsync(categories);
                        recordCount = categories.Count;
                        break;

                    case "customers":
                        var customers = await _dbContext.Customers.ToListAsync();
                        success = await _firestoreService.SyncCustomersAsync(customers);
                        recordCount = customers.Count;
                        break;

                    case "transactions":
                        var transactions = await _dbContext.Transactions
                            .Include(t => t.TransactionDetails)
                            .OrderByDescending(t => t.TransactionDate)
                            .Take(1000)
                            .ToListAsync();
                        success = await _firestoreService.SyncTransactionsAsync(transactions);
                        recordCount = transactions.Count;
                        break;

                    case "business_settings":
                        var settings = await _dbContext.BusinessSettings.ToListAsync();
                        success = await _firestoreService.SyncBusinessSettingsAsync(settings);
                        recordCount = settings.Count;
                        break;

                    default:
                        result.Success = false;
                        result.ErrorMessage = $"Unknown entity type: {entityType}";
                        return result;
                }

                result.Success = success;
                result.RecordCounts.Add(entityType, recordCount);

                // Log the sync activity
                await LogSyncActivity(deviceId, entityType, success, recordCount);

                result.EndTime = DateTime.UtcNow;
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing {EntityType} to Firebase", entityType);
                result.Success = false;
                result.ErrorMessage = ex.Message;
                result.EndTime = DateTime.UtcNow;
                return result;
            }
        }

        private async Task LogSyncActivity(string deviceId, string entityType, bool isSuccess, int recordCount)
        {
            // Log to local database
            var syncLog = new SyncLog
            {
                DeviceId = deviceId,
                EntityType = entityType,
                LastSyncTime = DateTime.UtcNow,
                IsSuccess = isSuccess,
                RecordsSynced = recordCount
            };

            _dbContext.SyncLogs.Add(syncLog);
            await _dbContext.SaveChangesAsync();

            // Log to Firestore
            await _firestoreService.LogSyncToFirestore(deviceId, entityType, isSuccess, recordCount);
        }
    }

    public class SyncResult
    {
        public bool Success { get; set; }
        public string? ErrorMessage { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public string DeviceId { get; set; } = string.Empty;
        public string? EntityType { get; set; }
        public Dictionary<string, int> RecordCounts { get; set; } = new Dictionary<string, int>();

        public TimeSpan Duration => EndTime - StartTime;
    }
}