using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using QuickTechDataSyncService.Data;
using QuickTechDataSyncService.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace QuickTechDataSyncService.Services
{
    /// <summary>
    /// An implementation of MongoDB synchronization that bypasses Entity Framework completely
    /// and works directly with ADO.NET and MongoDB BSON documents.
    /// </summary>
    public class DirectMongoDbService : IMongoDbSyncService
    {
        private readonly ApplicationDbContext _dbContext;
        private readonly ILogger<DirectMongoDbService> _logger;
        private readonly IConfiguration _configuration;
        private MongoClient _mongoClient;
        private IMongoDatabase _mongoDatabase;
        private bool _isInitialized = false;
        private readonly string _databaseName = "QuickTechPOS";

        public DirectMongoDbService(
            ApplicationDbContext dbContext,
            ILogger<DirectMongoDbService> logger,
            IConfiguration configuration)
        {
            _dbContext = dbContext;
            _logger = logger;
            _configuration = configuration;
        }

        public bool IsMongoInitialized => _isInitialized;

        public async Task<bool> InitializeMongoAsync()
        {
            try
            {
                string connectionString = _configuration.GetConnectionString("MongoDb");
                if (string.IsNullOrEmpty(connectionString))
                {
                    _logger.LogError("MongoDB connection string is missing in configuration");
                    return false;
                }

                _logger.LogInformation("Initializing MongoDB connection with connection string");
                var settings = MongoClientSettings.FromConnectionString(connectionString);
                settings.ServerSelectionTimeout = TimeSpan.FromSeconds(10);
                _mongoClient = new MongoClient(settings);

                _mongoDatabase = _mongoClient.GetDatabase(_databaseName);

                // Test the connection
                var result = await _mongoDatabase.RunCommandAsync<BsonDocument>(new BsonDocument("ping", 1));
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
                return false;
            }
        }

        public async Task<SyncResult> SyncAllDataToMongoAsync(string deviceId)
        {
            var result = new SyncResult
            {
                StartTime = DateTime.UtcNow,
                DeviceId = deviceId,
                EntityType = "All"
            };

            try
            {
                if (!_isInitialized)
                {
                    var initialized = await InitializeMongoAsync();
                    if (!initialized)
                    {
                        result.Success = false;
                        result.ErrorMessage = "Failed to initialize MongoDB";
                        result.EndTime = DateTime.UtcNow;
                        return result;
                    }
                }

                // Get connection
                var connection = _dbContext.Database.GetDbConnection() as SqlConnection;
                if (connection.State != ConnectionState.Open)
                {
                    await connection.OpenAsync();
                }

                // Sync each entity type
                await SyncCategoriesDirect(connection, result);
                await SyncProductsDirect(connection, result);
                await SyncCustomersDirect(connection, result);
                await SyncBusinessSettingsDirect(connection, result);
                await SyncTransactionsDirect(connection, result);

                await LogSyncActivity(deviceId, "All", true, result.RecordCounts.Values.Sum());

                // Fix for line 121 - Change from method group to method call  
                result.Success = result.RecordCounts.Values.Sum() > 0;
                result.EndTime = DateTime.UtcNow;
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncAllDataToMongoAsync: {Message}", ex.Message);
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
                if (!_isInitialized)
                {
                    var initialized = await InitializeMongoAsync();
                    if (!initialized)
                    {
                        result.Success = false;
                        result.ErrorMessage = "Failed to initialize MongoDB";
                        result.EndTime = DateTime.UtcNow;
                        return result;
                    }
                }

                // Get connection
                var connection = _dbContext.Database.GetDbConnection() as SqlConnection;
                if (connection.State != ConnectionState.Open)
                {
                    await connection.OpenAsync();
                }

                switch (entityType.ToLower())
                {
                    case "categories":
                        await SyncCategoriesDirect(connection, result);
                        break;

                    case "products":
                        await SyncProductsDirect(connection, result);
                        break;

                    case "customers":
                        await SyncCustomersDirect(connection, result);
                        break;

                    case "transactions":
                        await SyncTransactionsDirect(connection, result);
                        break;

                    case "business_settings":
                        await SyncBusinessSettingsDirect(connection, result);
                        break;

                    default:
                        result.Success = false;
                        result.ErrorMessage = $"Unknown entity type: {entityType}";
                        break;
                }

                // Log the activity if we have a count
                if (result.RecordCounts.TryGetValue(entityType, out int count))
                {
                    await LogSyncActivity(deviceId, entityType, result.Success, count);
                }

                result.EndTime = DateTime.UtcNow;
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncEntityToMongoAsync: {Message}", ex.Message);
                result.Success = false;
                result.ErrorMessage = ex.Message;
                result.EndTime = DateTime.UtcNow;
                return result;
            }
        }

        private async Task SyncCategoriesDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting direct sync of categories to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("categories");
                var bulkOps = new List<WriteModel<BsonDocument>>();
                var count = 0;

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT 
                            CategoryId, Name, Description, IsActive, Type
                        FROM 
                            Categories";

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            count++;
                            var doc = new BsonDocument
                            {
                                { "_id", reader.GetInt32(0) },
                                { "categoryId", reader.GetInt32(0) },
                                { "name", reader.GetString(1) },
                                { "description", reader.IsDBNull(2) ? string.Empty : reader.GetString(2) },
                                { "isActive", reader.GetBoolean(3) },
                                { "type", reader.IsDBNull(4) ? "Product" : reader.GetString(4) }
                            };

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", reader.GetInt32(0));
                            var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    var bulkResult = await collection.BulkWriteAsync(bulkOps);
                    _logger.LogInformation("Synced {Count} categories. Inserted: {Inserted}, Modified: {Modified}",
                        count, bulkResult.InsertedCount, bulkResult.ModifiedCount);
                }
                else
                {
                    _logger.LogInformation("No categories to sync");
                }

                result.RecordCounts["Categories"] = count;
                result.Success = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncCategoriesDirect: {Message}", ex.Message);
                result.RecordCounts["Categories"] = 0;
            }
        }

        private async Task SyncProductsDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting direct sync of products to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("products");
                var bulkOps = new List<WriteModel<BsonDocument>>();
                var count = 0;

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT 
                            p.ProductId, p.Barcode, p.Name, p.Description, p.CategoryId, 
                            p.PurchasePrice, p.SalePrice, p.CurrentStock, p.MinimumStock, 
                            p.SupplierId, p.IsActive, p.CreatedAt, p.Speed, p.UpdatedAt, p.ImagePath,
                            c.Name as CategoryName, c.Description as CategoryDescription, c.IsActive as CategoryIsActive, c.Type as CategoryType
                        FROM 
                            Products p
                        LEFT JOIN 
                            Categories c ON p.CategoryId = c.CategoryId";

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            count++;
                            var doc = new BsonDocument
                            {
                                { "_id", reader.GetInt32(0) },
                                { "productId", reader.GetInt32(0) },
                                { "barcode", reader.IsDBNull(1) ? string.Empty : reader.GetString(1) },
                                { "name", reader.GetString(2) },
                                { "description", reader.IsDBNull(3) ? string.Empty : reader.GetString(3) },
                                { "categoryId", reader.GetInt32(4) },
                                { "purchasePrice", reader.GetDecimal(5) },
                                { "salePrice", reader.GetDecimal(6) },
                                { "currentStock", reader.GetDecimal(7) },
                                { "minimumStock", reader.GetInt32(8) },
                                { "supplierId", reader.IsDBNull(9) ? BsonNull.Value : reader.GetInt32(9) },
                                { "isActive", reader.GetBoolean(10) },
                                { "createdAt", reader.GetDateTime(11) },
                                { "speed", reader.IsDBNull(12) ? string.Empty : reader.GetString(12) },
                                { "updatedAt", reader.IsDBNull(13) ? BsonNull.Value : reader.GetDateTime(13) },
                                { "imagePath", reader.IsDBNull(14) ? string.Empty : reader.GetString(14) }
                            };

                            // Add category info if available
                            if (!reader.IsDBNull(15)) // CategoryName field
                            {
                                doc.Add("category", new BsonDocument
                                {
                                    { "categoryId", reader.GetInt32(4) },
                                    { "name", reader.GetString(15) },
                                    { "description", reader.IsDBNull(16) ? string.Empty : reader.GetString(16) },
                                    { "isActive", reader.GetBoolean(17) },
                                    { "type", reader.IsDBNull(18) ? "Product" : reader.GetString(18) }
                                });
                            }

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", reader.GetInt32(0));
                            var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    var bulkResult = await collection.BulkWriteAsync(bulkOps);
                    _logger.LogInformation("Synced {Count} products. Inserted: {Inserted}, Modified: {Modified}",
                        count, bulkResult.InsertedCount, bulkResult.ModifiedCount);
                }
                else
                {
                    _logger.LogInformation("No products to sync");
                }

                result.RecordCounts["Products"] = count;
                result.Success = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncProductsDirect: {Message}", ex.Message);
                result.RecordCounts["Products"] = 0;
            }
        }

        private async Task SyncCustomersDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting direct sync of customers to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("customers");
                var bulkOps = new List<WriteModel<BsonDocument>>();
                var count = 0;

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT 
                            CustomerId, Name, Phone, Email, Address, IsActive, CreatedAt, UpdatedAt, Balance
                        FROM 
                            Customers";

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            count++;
                            var doc = new BsonDocument
                            {
                                { "_id", reader.GetInt32(0) },
                                { "customerId", reader.GetInt32(0) },
                                { "name", reader.GetString(1) },
                                { "phone", reader.IsDBNull(2) ? string.Empty : reader.GetString(2) },
                                { "email", reader.IsDBNull(3) ? string.Empty : reader.GetString(3) },
                                { "address", reader.IsDBNull(4) ? string.Empty : reader.GetString(4) },
                                { "isActive", reader.GetBoolean(5) },
                                { "createdAt", reader.GetDateTime(6) },
                                { "updatedAt", reader.IsDBNull(7) ? BsonNull.Value : reader.GetDateTime(7) },
                                { "balance", reader.GetDecimal(8) }
                            };

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", reader.GetInt32(0));
                            var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    var bulkResult = await collection.BulkWriteAsync(bulkOps);
                    _logger.LogInformation("Synced {Count} customers. Inserted: {Inserted}, Modified: {Modified}",
                        count, bulkResult.InsertedCount, bulkResult.ModifiedCount);
                }
                else
                {
                    _logger.LogInformation("No customers to sync");
                }

                result.RecordCounts["Customers"] = count;
                result.Success = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncCustomersDirect: {Message}", ex.Message);
                result.RecordCounts["Customers"] = 0;
            }
        }

        private async Task SyncBusinessSettingsDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting direct sync of business settings to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("business_settings");
                var bulkOps = new List<WriteModel<BsonDocument>>();
                var count = 0;

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT 
                            Id, [Key], Value, Description, [Group], DataType, IsSystem, LastModified, ModifiedBy
                        FROM 
                            BusinessSettings";

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            count++;
                            var doc = new BsonDocument
                            {
                                { "_id", reader.GetInt32(0) },
                                { "id", reader.GetInt32(0) },
                                { "key", reader.GetString(1) },
                                { "value", reader.GetString(2) },
                                { "description", reader.IsDBNull(3) ? string.Empty : reader.GetString(3) },
                                { "group", reader.IsDBNull(4) ? string.Empty : reader.GetString(4) },
                                { "dataType", reader.IsDBNull(5) ? "string" : reader.GetString(5) },
                                { "isSystem", reader.GetBoolean(6) },
                                { "lastModified", reader.GetDateTime(7) },
                                { "modifiedBy", reader.IsDBNull(8) ? string.Empty : reader.GetString(8) }
                            };

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", reader.GetInt32(0));
                            var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    var bulkResult = await collection.BulkWriteAsync(bulkOps);
                    _logger.LogInformation("Synced {Count} business settings. Inserted: {Inserted}, Modified: {Modified}",
                        count, bulkResult.InsertedCount, bulkResult.ModifiedCount);
                }
                else
                {
                    _logger.LogInformation("No business settings to sync");
                }

                result.RecordCounts["BusinessSettings"] = count;
                result.Success = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncBusinessSettingsDirect: {Message}", ex.Message);
                result.RecordCounts["BusinessSettings"] = 0;
            }
        }

        private async Task SyncTransactionsDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting direct sync of transactions to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("transactions");
                int count = 0;

                using (var command = connection.CreateCommand())
                {
                    // Taking most recent 100 transactions for performance
                    command.CommandText = @"
                        SELECT TOP 100 
                            TransactionId, CustomerId, CustomerName, TotalAmount, PaidAmount, 
                            TransactionDate, TransactionType, Status, PaymentMethod, 
                            CashierId, CashierName, CashierRole
                        FROM 
                            Transactions
                        ORDER BY 
                            TransactionDate DESC";

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            count++;
                            var transactionId = reader.GetInt32(0);

                            _logger.LogInformation("Processing transaction ID: {ID}", transactionId);

                            var doc = new BsonDocument
                            {
                                { "_id", transactionId },
                                { "transactionId", transactionId }
                            };

                            // Add nullable CustomerId
                            if (!reader.IsDBNull(1))
                            {
                                doc.Add("customerId", reader.GetInt32(1));
                            }
                            else
                            {
                                doc.Add("customerId", BsonNull.Value);
                            }

                            // Add remaining fields
                            doc.Add("customerName", reader.IsDBNull(2) ? string.Empty : reader.GetString(2));
                            doc.Add("totalAmount", reader.GetDecimal(3));
                            doc.Add("paidAmount", reader.GetDecimal(4));
                            doc.Add("transactionDate", reader.GetDateTime(5));

                            // Handle enum values by converting to string
                            int transactionTypeValue = reader.GetInt32(6);
                            int statusValue = reader.GetInt32(7);
                            doc.Add("transactionType", transactionTypeValue.ToString());
                            doc.Add("status", statusValue.ToString());

                            doc.Add("paymentMethod", reader.IsDBNull(8) ? string.Empty : reader.GetString(8));
                            doc.Add("cashierId", reader.IsDBNull(9) ? string.Empty : reader.GetString(9));
                            doc.Add("cashierName", reader.IsDBNull(10) ? string.Empty : reader.GetString(10));
                            doc.Add("cashierRole", reader.IsDBNull(11) ? string.Empty : reader.GetString(11));

                            // Get transaction details
                            var details = await GetTransactionDetailsDirect(connection, transactionId);
                            doc.Add("transactionDetails", details);

                            // Upsert directly instead of bulk to diagnose any potential issues
                            var filter = Builders<BsonDocument>.Filter.Eq("_id", transactionId);
                            await collection.ReplaceOneAsync(filter, doc, new ReplaceOptions { IsUpsert = true });

                            _logger.LogInformation("Successfully synced transaction {Id} with {DetailCount} details",
                                transactionId, details.Count);
                        }
                    }
                }

                _logger.LogInformation("Successfully synced {Count} transactions", count);
                result.RecordCounts["Transactions"] = count;
                result.Success = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncTransactionsDirect: {Message}", ex.Message);
                result.RecordCounts["Transactions"] = 0;
            }
        }

        private async Task<BsonArray> GetTransactionDetailsDirect(SqlConnection connection, int transactionId)
        {
            var details = new BsonArray();

            try
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT 
                            TransactionDetailId, TransactionId, ProductId, 
                            Quantity, UnitPrice, PurchasePrice, Discount, Total
                        FROM 
                            TransactionDetails
                        WHERE 
                            TransactionId = @TransactionId";

                    var param = new SqlParameter("@TransactionId", SqlDbType.Int) { Value = transactionId };
                    command.Parameters.Add(param);

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var detailDoc = new BsonDocument
                            {
                                { "transactionDetailId", reader.GetInt32(0) },
                                { "transactionId", reader.GetInt32(1) },
                                { "productId", reader.GetInt32(2) },
                                { "quantity", reader.GetDecimal(3) },
                                { "unitPrice", reader.GetDecimal(4) },
                                { "purchasePrice", reader.GetDecimal(5) },
                                { "discount", reader.GetDecimal(6) },
                                { "total", reader.GetDecimal(7) }
                            };

                            details.Add(detailDoc);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving transaction details for ID {TransactionId}: {Message}",
                    transactionId, ex.Message);
            }

            return details;
        }

        private async Task LogSyncActivity(string deviceId, string entityType, bool success, int recordCount)
        {
            try
            {
                var collection = _mongoDatabase.GetCollection<BsonDocument>("sync_logs");

                var syncLog = new BsonDocument
                {
                    { "_id", ObjectId.GenerateNewId() },
                    { "deviceId", deviceId },
                    { "entityType", entityType },
                    { "lastSyncTime", DateTime.UtcNow },
                    { "isSuccess", success },
                    { "recordsSynced", recordCount }
                };

                await collection.InsertOneAsync(syncLog);
                _logger.LogInformation("Logged sync activity for {EntityType}: {Count} records", entityType, recordCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error logging sync activity");
            }
        }
    }
}