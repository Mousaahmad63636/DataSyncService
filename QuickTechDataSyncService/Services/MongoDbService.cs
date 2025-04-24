using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;
using QuickTechDataSyncService.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QuickTechDataSyncService.Services
{
    public class MongoDbService : IMongoDbService
    {
        private readonly ILogger<MongoDbService> _logger;
        private readonly IConfiguration _configuration;
        private IMongoDatabase _database;
        private MongoClient _client;
        private bool _isInitialized = false;
        private readonly string _databaseName = "QuickTechPOS";

        public MongoDbService(ILogger<MongoDbService> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;

            var conventionPack = new ConventionPack { new CamelCaseElementNameConvention() };
            ConventionRegistry.Register("camelCase", conventionPack, t => true);
        }

        public bool IsInitialized => _isInitialized;

        public async Task<bool> InitializeAsync()
        {
            try
            {
                string connectionString = _configuration.GetConnectionString("MongoDb") ??
                    "mongodb+srv://username:password@cluster0.mongodb.net/";

                _client = new MongoClient(connectionString);

                var databaseList = await _client.ListDatabaseNames().ToListAsync();
                _logger.LogInformation("Connected to MongoDB. Available databases: {Databases}",
                    string.Join(", ", databaseList));

                _database = _client.GetDatabase(_databaseName);

                var result = await _database.RunCommandAsync<BsonDocument>(new BsonDocument("ping", 1));
                if (result["ok"] == 1)
                {
                    _logger.LogInformation("MongoDB connection verified successfully");
                    _isInitialized = true;
                    return true;
                }
                else
                {
                    _logger.LogError("MongoDB ping command failed");
                    return false;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize MongoDB: {Message}", ex.Message);
                _isInitialized = false;
                return false;
            }
        }

        public async Task<bool> SyncProductsAsync(IEnumerable<Product> products)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("MongoDB not initialized. Call InitializeAsync first.");
                return false;
            }

            try
            {
                var collection = _database.GetCollection<Product>("products");

                var bulkOperations = new List<WriteModel<Product>>();

                foreach (var product in products)
                {
                    var filter = Builders<Product>.Filter.Eq(p => p.ProductId, product.ProductId);
                    var upsert = new ReplaceOneModel<Product>(filter, product) { IsUpsert = true };
                    bulkOperations.Add(upsert);
                }

                if (bulkOperations.Any())
                {
                    var result = await collection.BulkWriteAsync(bulkOperations);
                    _logger.LogInformation("Successfully synced {Count} products to MongoDB. Inserted: {Inserted}, Modified: {Modified}",
                        products.Count(), result.InsertedCount, result.ModifiedCount);
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing products to MongoDB");
                return false;
            }
        }

        public async Task<List<Product>> GetProductsAsync(DateTime? lastSyncTime = null)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("MongoDB not initialized. Call InitializeAsync first.");
                return new List<Product>();
            }

            try
            {
                var collection = _database.GetCollection<Product>("products");

                FilterDefinition<Product> filter;
                if (lastSyncTime.HasValue)
                {
                    filter = Builders<Product>.Filter.Gte(p => p.UpdatedAt, lastSyncTime.Value);
                }
                else
                {
                    filter = Builders<Product>.Filter.Empty;
                }

                var products = await collection.Find(filter).ToListAsync();
                _logger.LogInformation("Successfully retrieved {Count} products from MongoDB", products.Count);

                return products;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving products from MongoDB");
                return new List<Product>();
            }
        }

        public async Task<bool> SyncCategoriesAsync(IEnumerable<Category> categories)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("MongoDB not initialized. Call InitializeAsync first.");
                return false;
            }

            try
            {
                var collection = _database.GetCollection<Category>("categories");

                var bulkOperations = new List<WriteModel<Category>>();

                foreach (var category in categories)
                {
                    var filter = Builders<Category>.Filter.Eq(c => c.CategoryId, category.CategoryId);
                    var upsert = new ReplaceOneModel<Category>(filter, category) { IsUpsert = true };
                    bulkOperations.Add(upsert);
                }

                if (bulkOperations.Any())
                {
                    var result = await collection.BulkWriteAsync(bulkOperations);
                    _logger.LogInformation("Successfully synced {Count} categories to MongoDB", categories.Count());
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing categories to MongoDB");
                return false;
            }
        }

        public async Task<bool> SyncCustomersAsync(IEnumerable<Customer> customers)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("MongoDB not initialized. Call InitializeAsync first.");
                return false;
            }

            try
            {
                var collection = _database.GetCollection<Customer>("customers");

                var bulkOperations = new List<WriteModel<Customer>>();

                foreach (var customer in customers)
                {
                    var filter = Builders<Customer>.Filter.Eq(c => c.CustomerId, customer.CustomerId);
                    var upsert = new ReplaceOneModel<Customer>(filter, customer) { IsUpsert = true };
                    bulkOperations.Add(upsert);
                }

                if (bulkOperations.Any())
                {
                    var result = await collection.BulkWriteAsync(bulkOperations);
                    _logger.LogInformation("Successfully synced {Count} customers to MongoDB", customers.Count());
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing customers to MongoDB");
                return false;
            }
        }

        public async Task<bool> SyncTransactionsAsync(IEnumerable<Transaction> transactions)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("MongoDB not initialized. Call InitializeAsync first.");
                return false;
            }

            try
            {
                var collection = _database.GetCollection<Transaction>("transactions");

                var bulkOperations = new List<WriteModel<Transaction>>();

                foreach (var transaction in transactions)
                {
                    var filter = Builders<Transaction>.Filter.Eq(t => t.TransactionId, transaction.TransactionId);
                    var upsert = new ReplaceOneModel<Transaction>(filter, transaction) { IsUpsert = true };
                    bulkOperations.Add(upsert);
                }

                if (bulkOperations.Any())
                {
                    var result = await collection.BulkWriteAsync(bulkOperations);
                    _logger.LogInformation("Successfully synced {Count} transactions to MongoDB", transactions.Count());
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing transactions to MongoDB");
                return false;
            }
        }

        public async Task<bool> SyncBusinessSettingsAsync(IEnumerable<BusinessSetting> settings)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("MongoDB not initialized. Call InitializeAsync first.");
                return false;
            }

            try
            {
                var collection = _database.GetCollection<BusinessSetting>("business_settings");

                var bulkOperations = new List<WriteModel<BusinessSetting>>();

                foreach (var setting in settings)
                {
                    var filter = Builders<BusinessSetting>.Filter.Eq(s => s.Key, setting.Key);
                    var upsert = new ReplaceOneModel<BusinessSetting>(filter, setting) { IsUpsert = true };
                    bulkOperations.Add(upsert);
                }

                if (bulkOperations.Any())
                {
                    var result = await collection.BulkWriteAsync(bulkOperations);
                    _logger.LogInformation("Successfully synced {Count} business settings to MongoDB", settings.Count());
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing business settings to MongoDB");
                return false;
            }
        }

        public async Task LogSyncActivityAsync(string deviceId, string entityType, bool success, int recordCount)
        {
            if (!_isInitialized) return;

            try
            {
                var syncLog = new SyncLog
                {
                    DeviceId = deviceId,
                    EntityType = entityType,
                    LastSyncTime = DateTime.UtcNow,
                    IsSuccess = success,
                    RecordsSynced = recordCount
                };

                var collection = _database.GetCollection<SyncLog>("sync_logs");
                await collection.InsertOneAsync(syncLog);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error logging sync activity to MongoDB");
            }
        }
    }
}