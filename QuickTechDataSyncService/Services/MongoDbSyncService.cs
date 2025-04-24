using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using QuickTechDataSyncService.Data;
using QuickTechDataSyncService.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QuickTechDataSyncService.Services
{
    public class MongoDbSyncService : IMongoDbSyncService
    {
        private readonly ApplicationDbContext _dbContext;
        private readonly IMongoDbService _mongoDbService;
        private readonly ILogger<MongoDbSyncService> _logger;

        public MongoDbSyncService(
            ApplicationDbContext dbContext,
            IMongoDbService mongoDbService,
            ILogger<MongoDbSyncService> logger)
        {
            _dbContext = dbContext;
            _mongoDbService = mongoDbService;
            _logger = logger;
        }

        public bool IsMongoInitialized => _mongoDbService.IsInitialized;

        public async Task<bool> InitializeMongoAsync()
        {
            return await _mongoDbService.InitializeAsync();
        }

        public async Task<SyncResult> SyncAllDataToMongoAsync(string deviceId)
        {
            var result = new SyncResult
            {
                StartTime = DateTime.UtcNow,
                DeviceId = deviceId
            };

            try
            {
                if (!_mongoDbService.IsInitialized)
                {
                    var initialized = await _mongoDbService.InitializeAsync();
                    if (!initialized)
                    {
                        result.Success = false;
                        result.ErrorMessage = "Failed to initialize MongoDB";
                        return result;
                    }
                }

                var products = await _dbContext.Products.Include(p => p.Category).ToListAsync();
                var productsSuccess = await _mongoDbService.SyncProductsAsync(products);
                result.RecordCounts.Add("Products", products.Count);

                var categories = await _dbContext.Categories.ToListAsync();
                var categoriesSuccess = await _mongoDbService.SyncCategoriesAsync(categories);
                result.RecordCounts.Add("Categories", categories.Count);

                var customers = await _dbContext.Customers.ToListAsync();
                var customersSuccess = await _mongoDbService.SyncCustomersAsync(customers);
                result.RecordCounts.Add("Customers", customers.Count);

                var settings = await _dbContext.BusinessSettings.ToListAsync();
                var settingsSuccess = await _mongoDbService.SyncBusinessSettingsAsync(settings);
                result.RecordCounts.Add("BusinessSettings", settings.Count);

                var recentTransactions = await _dbContext.Transactions
                    .Include(t => t.TransactionDetails)
                    .OrderByDescending(t => t.TransactionDate)
                    .Take(1000)
                    .ToListAsync();

                var transactionsSuccess = await _mongoDbService.SyncTransactionsAsync(recentTransactions);
                result.RecordCounts.Add("Transactions", recentTransactions.Count);

                result.Success = productsSuccess && categoriesSuccess && customersSuccess &&
                                settingsSuccess && transactionsSuccess;

                await _mongoDbService.LogSyncActivityAsync(deviceId, "All", result.Success,
                    result.RecordCounts.Values.Sum());

                result.EndTime = DateTime.UtcNow;
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during full MongoDB sync");
                result.Success = false;
                result.ErrorMessage = ex.Message;
                result.EndTime = DateTime.UtcNow;
                return result;
            }
        }

        public async Task<SyncResult> SyncEntityToMongoAsync(string deviceId, string entityType)
        {
            var result = new SyncResult
            {
                StartTime = DateTime.UtcNow,
                DeviceId = deviceId,
                EntityType = entityType
            };

            try
            {
                if (!_mongoDbService.IsInitialized)
                {
                    var initialized = await _mongoDbService.InitializeAsync();
                    if (!initialized)
                    {
                        result.Success = false;
                        result.ErrorMessage = "Failed to initialize MongoDB";
                        return result;
                    }
                }

                int recordCount = 0;
                bool success = false;

                switch (entityType.ToLower())
                {
                    case "products":
                        var products = await _dbContext.Products.Include(p => p.Category).ToListAsync();
                        success = await _mongoDbService.SyncProductsAsync(products);
                        recordCount = products.Count;
                        break;

                    case "categories":
                        var categories = await _dbContext.Categories.ToListAsync();
                        success = await _mongoDbService.SyncCategoriesAsync(categories);
                        recordCount = categories.Count;
                        break;

                    case "customers":
                        var customers = await _dbContext.Customers.ToListAsync();
                        success = await _mongoDbService.SyncCustomersAsync(customers);
                        recordCount = customers.Count;
                        break;

                    case "transactions":
                        var transactions = await _dbContext.Transactions
                            .Include(t => t.TransactionDetails)
                            .OrderByDescending(t => t.TransactionDate)
                            .Take(1000)
                            .ToListAsync();
                        success = await _mongoDbService.SyncTransactionsAsync(transactions);
                        recordCount = transactions.Count;
                        break;

                    case "business_settings":
                        var settings = await _dbContext.BusinessSettings.ToListAsync();
                        success = await _mongoDbService.SyncBusinessSettingsAsync(settings);
                        recordCount = settings.Count;
                        break;

                    default:
                        result.Success = false;
                        result.ErrorMessage = $"Unknown entity type: {entityType}";
                        return result;
                }

                result.Success = success;
                result.RecordCounts.Add(entityType, recordCount);

                await _mongoDbService.LogSyncActivityAsync(deviceId, entityType, success, recordCount);

                result.EndTime = DateTime.UtcNow;
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing {EntityType} to MongoDB", entityType);
                result.Success = false;
                result.ErrorMessage = ex.Message;
                result.EndTime = DateTime.UtcNow;
                return result;
            }
        }
    }
}