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
using System.Linq;
using System.Threading.Tasks;

namespace QuickTechDataSyncService.Services
{
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

                _logger.LogInformation("Initializing MongoDB connection with extended timeout for slow internet");
                var settings = MongoClientSettings.FromConnectionString(connectionString);
                settings.ServerSelectionTimeout = TimeSpan.FromSeconds(30);
                settings.ConnectTimeout = TimeSpan.FromSeconds(30);
                settings.SocketTimeout = TimeSpan.FromMinutes(10);
                _mongoClient = new MongoClient(settings);

                _mongoDatabase = _mongoClient.GetDatabase(_databaseName);

                var result = await _mongoDatabase.RunCommandAsync<BsonDocument>(new BsonDocument("ping", 1));
                if (result["ok"] == 1)
                {
                    _logger.LogInformation("MongoDB connection verified successfully with slow internet optimizations");
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

        private async Task<SyncCheckpoint?> GetSyncCheckpoint(string deviceId, string entityType)
        {
            try
            {
                return await _dbContext.SyncCheckpoints
                    .FirstOrDefaultAsync(sc => sc.DeviceId == deviceId && sc.EntityType == entityType);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting sync checkpoint for {DeviceId} - {EntityType}", deviceId, entityType);
                return null;
            }
        }

        private async Task UpdateSyncCheckpoint(string deviceId, string entityType, DateTime syncTime, int? lastRecordId = null, string? checkpointData = null)
        {
            try
            {
                var checkpoint = await _dbContext.SyncCheckpoints
                    .FirstOrDefaultAsync(sc => sc.DeviceId == deviceId && sc.EntityType == entityType);

                if (checkpoint == null)
                {
                    checkpoint = new SyncCheckpoint
                    {
                        DeviceId = deviceId,
                        EntityType = entityType,
                        LastSyncTime = syncTime,
                        LastRecordId = lastRecordId ?? 0,
                        CheckpointData = checkpointData,
                        CreatedAt = DateTime.UtcNow
                    };
                    _dbContext.SyncCheckpoints.Add(checkpoint);
                }
                else
                {
                    checkpoint.LastSyncTime = syncTime;
                    if (lastRecordId.HasValue)
                        checkpoint.LastRecordId = lastRecordId.Value;
                    if (checkpointData != null)
                        checkpoint.CheckpointData = checkpointData;
                    checkpoint.UpdatedAt = DateTime.UtcNow;
                }

                await _dbContext.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating sync checkpoint for {DeviceId} - {EntityType}", deviceId, entityType);
            }
        }

        private async Task<int> SyncDeletedTransactions(SqlConnection connection, IMongoCollection<BsonDocument> collection, DateTime lastSyncTime)
        {
            try
            {
                var deletedIds = new List<int>();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT TransactionId 
                        FROM Transactions 
                        WHERE IsDeleted = 1 AND ModifiedDate > @LastSyncTime";

                    command.Parameters.Add(new SqlParameter("@LastSyncTime", lastSyncTime));
                    command.CommandTimeout = 120;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            deletedIds.Add(reader.GetInt32(0));
                        }
                    }
                }

                if (deletedIds.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", deletedIds);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);

                    _logger.LogInformation("Cleaned up {Count} deleted transactions from MongoDB", deleteResult.DeletedCount);
                    return (int)deleteResult.DeletedCount;
                }

                return 0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing deleted transactions: {Message}", ex.Message);
                return 0;
            }
        }

        private async Task SyncTransactionsDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting optimized incremental sync of transactions to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("transactions");

                var checkpoint = await GetSyncCheckpoint(result.DeviceId, "Transactions");
                var lastSyncTime = checkpoint?.LastSyncTime ?? DateTime.UtcNow.AddDays(-3);

                _logger.LogInformation("Syncing transactions modified after: {LastSyncTime}", lastSyncTime);

                const int batchSize = 100;
                int totalCount = 0;
                int deletedCount = 0;
                int batchNumber = 0;
                DateTime maxModifiedDate = lastSyncTime;
                bool hasMoreData = true;

                if (checkpoint != null)
                {
                    _logger.LogInformation("Checking for deleted transactions...");
                    deletedCount = await SyncDeletedTransactions(connection, collection, lastSyncTime);
                    result.RecordCounts["DeletedTransactions"] = deletedCount;
                }

                int totalEstimate = await GetTransactionCount(connection, lastSyncTime);
                _logger.LogInformation("Estimated {Count} transactions to sync in batches of {BatchSize}", totalEstimate, batchSize);

                while (hasMoreData)
                {
                    batchNumber++;
                    var bulkOps = new List<WriteModel<BsonDocument>>();

                    _logger.LogInformation("Processing batch {BatchNumber} of approximately {TotalBatches}...",
                        batchNumber, Math.Ceiling((double)totalEstimate / batchSize));

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"
                            SELECT TOP (@BatchSize)
                                TransactionId, CustomerId, CustomerName, TotalAmount, PaidAmount, 
                                TransactionDate, TransactionType, Status, PaymentMethod, 
                                CashierId, CashierName, CashierRole, CreatedDate, ModifiedDate
                            FROM Transactions
                            WHERE ModifiedDate > @LastSyncTime AND IsDeleted = 0
                            ORDER BY ModifiedDate ASC, TransactionId ASC";

                        command.Parameters.Add(new SqlParameter("@BatchSize", batchSize));
                        command.Parameters.Add(new SqlParameter("@LastSyncTime", lastSyncTime));
                        command.CommandTimeout = 300;

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            int batchCount = 0;

                            while (await reader.ReadAsync())
                            {
                                batchCount++;
                                var transactionId = reader.GetInt32(0);
                                var modifiedDate = reader.GetDateTime(13);

                                if (modifiedDate > maxModifiedDate)
                                    maxModifiedDate = modifiedDate;

                                try
                                {
                                    var doc = new BsonDocument
                                    {
                                        { "_id", transactionId },
                                        { "transactionId", transactionId }
                                    };

                                    if (!reader.IsDBNull(1))
                                        doc.Add("customerId", reader.GetInt32(1));
                                    else
                                        doc.Add("customerId", BsonNull.Value);

                                    doc.Add("customerName", reader.IsDBNull(2) ? string.Empty : reader.GetString(2));
                                    doc.Add("totalAmount", BsonDecimal128.Create(reader.GetDecimal(3)));
                                    doc.Add("paidAmount", BsonDecimal128.Create(reader.GetDecimal(4)));
                                    doc.Add("transactionDate", reader.GetDateTime(5).ToUniversalTime());
                                    doc.Add("transactionType", GetEnumName(typeof(TransactionType), reader.GetInt32(6)));
                                    doc.Add("status", GetEnumName(typeof(TransactionStatus), reader.GetInt32(7)));
                                    doc.Add("paymentMethod", reader.IsDBNull(8) ? string.Empty : reader.GetString(8));
                                    doc.Add("cashierId", reader.IsDBNull(9) ? string.Empty : reader.GetString(9));
                                    doc.Add("cashierName", reader.IsDBNull(10) ? string.Empty : reader.GetString(10));
                                    doc.Add("cashierRole", reader.IsDBNull(11) ? string.Empty : reader.GetString(11));
                                    doc.Add("createdDate", reader.GetDateTime(12).ToUniversalTime());
                                    doc.Add("modifiedDate", modifiedDate.ToUniversalTime());
                                    doc.Add("syncedAt", DateTime.UtcNow);

                                    var details = await GetTransactionDetailsDirect(connection, transactionId);
                                    doc.Add("transactionDetails", details);

                                    var filter = Builders<BsonDocument>.Filter.Eq("_id", transactionId);
                                    var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                                    bulkOps.Add(upsert);
                                }
                                catch (Exception ex)
                                {
                                    _logger.LogError(ex, "Error processing transaction {TransactionId} in batch {BatchNumber}", transactionId, batchNumber);
                                }
                            }

                            hasMoreData = batchCount == batchSize;
                        }
                    }

                    if (bulkOps.Count > 0)
                    {
                        try
                        {
                            _logger.LogInformation("Uploading batch {BatchNumber} with {Count} transactions to MongoDB...", batchNumber, bulkOps.Count);

                            var bulkResult = await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
                            totalCount += bulkOps.Count;

                            await UpdateSyncCheckpoint(result.DeviceId, "Transactions", maxModifiedDate);

                            _logger.LogInformation("Batch {BatchNumber} completed successfully. Progress: {Current}/{Total} transactions",
                                batchNumber, totalCount, totalEstimate);

                            if (hasMoreData)
                                await Task.Delay(1000);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error uploading batch {BatchNumber} to MongoDB: {Message}", batchNumber, ex.Message);
                            result.Success = false;
                            result.ErrorMessage = $"Failed at batch {batchNumber}: {ex.Message}";
                            return;
                        }
                    }
                }

                _logger.LogInformation("Transaction sync completed successfully. Total: {Count} synced, {Deleted} deleted",
                    totalCount, deletedCount);

                result.RecordCounts["Transactions"] = totalCount;
                result.Success = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Critical error in SyncTransactionsDirect: {Message}", ex.Message);
                result.RecordCounts["Transactions"] = 0;
                result.Success = false;
                result.ErrorMessage = ex.Message;
            }
        }

        private async Task<int> GetTransactionCount(SqlConnection connection, DateTime lastSyncTime)
        {
            try
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT COUNT(*) 
                        FROM Transactions 
                        WHERE ModifiedDate > @LastSyncTime AND IsDeleted = 0";

                    command.Parameters.Add(new SqlParameter("@LastSyncTime", lastSyncTime));
                    command.CommandTimeout = 60;

                    var result = await command.ExecuteScalarAsync();
                    return Convert.ToInt32(result);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting transaction count");
                return 0;
            }
        }

        private string GetEnumName(Type enumType, int value)
        {
            try
            {
                return Enum.GetName(enumType, value) ?? $"Unknown({value})";
            }
            catch
            {
                return $"Invalid({value})";
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
                    command.CommandTimeout = 120;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            try
                            {
                                var detailDoc = new BsonDocument
                                {
                                    { "transactionDetailId", reader.GetInt32(0) },
                                    { "transactionId", reader.GetInt32(1) },
                                    { "productId", reader.GetInt32(2) },
                                    { "quantity", BsonDecimal128.Create(reader.GetDecimal(3)) },
                                    { "unitPrice", BsonDecimal128.Create(reader.GetDecimal(4)) },
                                    { "purchasePrice", BsonDecimal128.Create(reader.GetDecimal(5)) },
                                    { "discount", BsonDecimal128.Create(reader.GetDecimal(6)) },
                                    { "total", BsonDecimal128.Create(reader.GetDecimal(7)) }
                                };

                                details.Add(detailDoc);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Error retrieving details for transaction {TransactionId}: {Message}", transactionId, ex.Message);
                            }
                        }
                    }
                }

                return details;
            }
            catch (Exception)
            {
                return details;
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

                var connection = _dbContext.Database.GetDbConnection() as SqlConnection;
                if (connection.State != ConnectionState.Open)
                {
                    await connection.OpenAsync();
                }

                _logger.LogInformation("Starting optimized full data sync with 3-day window and small batches");

                await SyncCategoriesDirect(connection, result);
                await SyncProductsDirect(connection, result);
                await SyncCustomersDirect(connection, result);
                await SyncBusinessSettingsDirect(connection, result);
                await SyncTransactionsDirect(connection, result);
                await SyncExpensesDirect(connection, result);
                await SyncEmployeesDirect(connection, result);

                await LogSyncActivity(deviceId, "All", true, result.RecordCounts.Values.Sum());

                result.Success = true;
                result.EndTime = DateTime.UtcNow;
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in optimized SyncAllDataToMongoAsync: {Message}", ex.Message);
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
                    case "expenses":
                        await SyncExpensesDirect(connection, result);
                        break;
                    case "employees":
                        await SyncEmployeesDirect(connection, result);
                        break;
                    default:
                        result.Success = false;
                        result.ErrorMessage = $"Unknown entity type: {entityType}";
                        break;
                }

                if (result.Success && result.RecordCounts.TryGetValue(entityType, out int count))
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
                result.ErrorMessage = $"{entityType} sync failed: {ex.Message}";
                result.EndTime = DateTime.UtcNow;
                return result;
            }
        }

        private async Task SyncCategoriesDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting sync of categories to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("categories");

                int totalCount = 0;
                var bulkOps = new List<WriteModel<BsonDocument>>();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT CategoryId, Name, Description, IsActive, Type
                        FROM Categories
                        WHERE IsActive = 1";
                    command.CommandTimeout = 120;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            totalCount++;
                            var categoryId = reader.GetInt32(0);

                            var doc = new BsonDocument
                            {
                                { "_id", categoryId },
                                { "categoryId", categoryId },
                                { "name", reader.GetString(1) },
                                { "description", reader.IsDBNull(2) ? string.Empty : reader.GetString(2) },
                                { "isActive", reader.GetBoolean(3) },
                                { "type", reader.IsDBNull(4) ? "Product" : reader.GetString(4) },
                                { "syncedAt", DateTime.UtcNow }
                            };

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", categoryId);
                            var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
                    await UpdateSyncCheckpoint(result.DeviceId, "Categories", DateTime.UtcNow);
                }

                result.RecordCounts["Categories"] = totalCount;
                result.Success = true;
                _logger.LogInformation("Categories sync completed: {Count} records", totalCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncCategoriesDirect: {Message}", ex.Message);
                result.RecordCounts["Categories"] = 0;
                result.Success = false;
            }
        }

        private async Task SyncProductsDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting optimized sync of products to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("products");

                var checkpoint = await GetSyncCheckpoint(result.DeviceId, "Products");
                var lastSyncTime = checkpoint?.LastSyncTime ?? DateTime.UtcNow.AddDays(-3);

                const int batchSize = 200;
                int totalCount = 0;
                DateTime maxModifiedDate = lastSyncTime;
                bool hasMoreData = true;
                int batchNumber = 0;

                while (hasMoreData)
                {
                    batchNumber++;
                    var bulkOps = new List<WriteModel<BsonDocument>>();

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"
                            SELECT TOP (@BatchSize)
                                p.ProductId, p.Barcode, p.Name, p.Description, p.CategoryId, 
                                p.PurchasePrice, p.SalePrice, p.CurrentStock, p.MinimumStock, 
                                p.SupplierId, p.IsActive, p.CreatedAt, p.Speed, p.UpdatedAt, p.ImagePath,
                                c.Name as CategoryName
                            FROM Products p
                            LEFT JOIN Categories c ON p.CategoryId = c.CategoryId
                            WHERE (p.UpdatedAt > @LastSyncTime OR p.UpdatedAt IS NULL) AND p.IsActive = 1
                            ORDER BY ISNULL(p.UpdatedAt, p.CreatedAt) ASC, p.ProductId ASC";

                        command.Parameters.Add(new SqlParameter("@BatchSize", batchSize));
                        command.Parameters.Add(new SqlParameter("@LastSyncTime", lastSyncTime));
                        command.CommandTimeout = 180;

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            int batchCount = 0;

                            while (await reader.ReadAsync())
                            {
                                batchCount++;
                                var productId = reader.GetInt32(0);
                                var updatedAt = reader.IsDBNull(13) ? reader.GetDateTime(11) : reader.GetDateTime(13);

                                if (updatedAt > maxModifiedDate)
                                    maxModifiedDate = updatedAt;

                                var doc = new BsonDocument
                                {
                                    { "_id", productId },
                                    { "productId", productId },
                                    { "barcode", reader.IsDBNull(1) ? string.Empty : reader.GetString(1) },
                                    { "name", reader.GetString(2) },
                                    { "categoryId", reader.GetInt32(4) },
                                    { "purchasePrice", BsonDecimal128.Create(reader.GetDecimal(5)) },
                                    { "salePrice", BsonDecimal128.Create(reader.GetDecimal(6)) },
                                    { "currentStock", BsonDecimal128.Create(reader.GetDecimal(7)) },
                                    { "isActive", reader.GetBoolean(10) },
                                    { "syncedAt", DateTime.UtcNow }
                                };

                                var filter = Builders<BsonDocument>.Filter.Eq("_id", productId);
                                var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                                bulkOps.Add(upsert);
                            }

                            hasMoreData = batchCount == batchSize;
                        }
                    }

                    if (bulkOps.Count > 0)
                    {
                        await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
                        totalCount += bulkOps.Count;
                        await UpdateSyncCheckpoint(result.DeviceId, "Products", maxModifiedDate);

                        _logger.LogInformation("Products batch {BatchNumber} completed: {Count} records", batchNumber, bulkOps.Count);

                        if (hasMoreData)
                            await Task.Delay(200);
                    }
                }

                result.RecordCounts["Products"] = totalCount;
                result.Success = true;
                _logger.LogInformation("Products sync completed: {Count} total records", totalCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncProductsDirect: {Message}", ex.Message);
                result.RecordCounts["Products"] = 0;
                result.Success = false;
            }
        }

        private async Task SyncCustomersDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting sync of customers to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("customers");

                var checkpoint = await GetSyncCheckpoint(result.DeviceId, "Customers");
                var lastSyncTime = checkpoint?.LastSyncTime ?? DateTime.UtcNow.AddDays(-3);

                const int batchSize = 200;
                int totalCount = 0;
                DateTime maxModifiedDate = lastSyncTime;
                bool hasMoreData = true;

                while (hasMoreData)
                {
                    var bulkOps = new List<WriteModel<BsonDocument>>();

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"
                            SELECT TOP (@BatchSize)
                                CustomerId, Name, Phone, Email, Address, IsActive, CreatedAt, UpdatedAt, Balance
                            FROM Customers
                            WHERE (UpdatedAt > @LastSyncTime OR UpdatedAt IS NULL) AND IsActive = 1
                            ORDER BY ISNULL(UpdatedAt, CreatedAt) ASC, CustomerId ASC";

                        command.Parameters.Add(new SqlParameter("@BatchSize", batchSize));
                        command.Parameters.Add(new SqlParameter("@LastSyncTime", lastSyncTime));
                        command.CommandTimeout = 120;

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            int batchCount = 0;

                            while (await reader.ReadAsync())
                            {
                                batchCount++;
                                var customerId = reader.GetInt32(0);
                                var updatedAt = reader.IsDBNull(7) ? reader.GetDateTime(6) : reader.GetDateTime(7);

                                if (updatedAt > maxModifiedDate)
                                    maxModifiedDate = updatedAt;

                                var doc = new BsonDocument
                                {
                                    { "_id", customerId },
                                    { "customerId", customerId },
                                    { "name", reader.GetString(1) },
                                    { "phone", reader.IsDBNull(2) ? string.Empty : reader.GetString(2) },
                                    { "email", reader.IsDBNull(3) ? string.Empty : reader.GetString(3) },
                                    { "isActive", reader.GetBoolean(5) },
                                    { "balance", BsonDecimal128.Create(reader.GetDecimal(8)) },
                                    { "syncedAt", DateTime.UtcNow }
                                };

                                var filter = Builders<BsonDocument>.Filter.Eq("_id", customerId);
                                var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                                bulkOps.Add(upsert);
                            }

                            hasMoreData = batchCount == batchSize;
                        }
                    }

                    if (bulkOps.Count > 0)
                    {
                        await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
                        totalCount += bulkOps.Count;
                        await UpdateSyncCheckpoint(result.DeviceId, "Customers", maxModifiedDate);

                        if (hasMoreData)
                            await Task.Delay(200);
                    }
                }

                result.RecordCounts["Customers"] = totalCount;
                result.Success = true;
                _logger.LogInformation("Customers sync completed: {Count} records", totalCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncCustomersDirect: {Message}", ex.Message);
                result.RecordCounts["Customers"] = 0;
                result.Success = false;
            }
        }

        private async Task SyncBusinessSettingsDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting sync of business settings to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("business_settings");

                var checkpoint = await GetSyncCheckpoint(result.DeviceId, "BusinessSettings");
                var lastSyncTime = checkpoint?.LastSyncTime ?? DateTime.UtcNow.AddDays(-3);

                int totalCount = 0;
                var bulkOps = new List<WriteModel<BsonDocument>>();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT Id, [Key], Value, Description, [Group], DataType, IsSystem, LastModified, ModifiedBy
                        FROM BusinessSettings
                        WHERE LastModified > @LastSyncTime
                        ORDER BY LastModified ASC";

                    command.Parameters.Add(new SqlParameter("@LastSyncTime", lastSyncTime));
                    command.CommandTimeout = 120;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            totalCount++;
                            var settingId = reader.GetInt32(0);

                            var doc = new BsonDocument
                            {
                                { "_id", settingId },
                                { "id", settingId },
                                { "key", reader.GetString(1) },
                                { "value", reader.GetString(2) },
                                { "isSystem", reader.GetBoolean(6) },
                                { "lastModified", reader.GetDateTime(7).ToUniversalTime() },
                                { "syncedAt", DateTime.UtcNow }
                            };

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", settingId);
                            var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
                    await UpdateSyncCheckpoint(result.DeviceId, "BusinessSettings", DateTime.UtcNow);
                }

                result.RecordCounts["BusinessSettings"] = totalCount;
                result.Success = true;
                _logger.LogInformation("Business settings sync completed: {Count} records", totalCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncBusinessSettingsDirect: {Message}", ex.Message);
                result.RecordCounts["BusinessSettings"] = 0;
                result.Success = false;
            }
        }

        public async Task<SyncResult> SyncExpensesToMongoAsync(string deviceId)
        {
            var result = new SyncResult
            {
                StartTime = DateTime.UtcNow,
                DeviceId = deviceId,
                EntityType = "Expenses"
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

                var connection = _dbContext.Database.GetDbConnection() as SqlConnection;
                if (connection.State != ConnectionState.Open)
                {
                    await connection.OpenAsync();
                }

                await SyncExpensesDirect(connection, result);

                result.EndTime = DateTime.UtcNow;
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncExpensesToMongoAsync: {Message}", ex.Message);
                result.Success = false;
                result.ErrorMessage = $"Expense sync failed: {ex.Message}";
                result.EndTime = DateTime.UtcNow;
                return result;
            }
        }

        public async Task<SyncResult> SyncEmployeesToMongoAsync(string deviceId)
        {
            var result = new SyncResult
            {
                StartTime = DateTime.UtcNow,
                DeviceId = deviceId,
                EntityType = "Employees"
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

                var connection = _dbContext.Database.GetDbConnection() as SqlConnection;
                if (connection.State != ConnectionState.Open)
                {
                    await connection.OpenAsync();
                }

                await SyncEmployeesDirect(connection, result);

                result.EndTime = DateTime.UtcNow;
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncEmployeesToMongoAsync: {Message}", ex.Message);
                result.Success = false;
                result.ErrorMessage = $"Employee sync failed: {ex.Message}";
                result.EndTime = DateTime.UtcNow;
                return result;
            }
        }

        private async Task SyncExpensesDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting sync of expenses to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("expenses");

                var checkpoint = await GetSyncCheckpoint(result.DeviceId, "Expenses");
                var lastSyncTime = checkpoint?.LastSyncTime ?? DateTime.UtcNow.AddDays(-3);

                int totalCount = 0;
                var bulkOps = new List<WriteModel<BsonDocument>>();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT ExpenseId, Reason, Amount, Date, Notes, Category, IsRecurring, CreatedAt, UpdatedAt
                        FROM Expenses
                        WHERE UpdatedAt > @LastSyncTime OR UpdatedAt IS NULL
                        ORDER BY ISNULL(UpdatedAt, CreatedAt) ASC";

                    command.Parameters.Add(new SqlParameter("@LastSyncTime", lastSyncTime));
                    command.CommandTimeout = 120;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            totalCount++;
                            var expenseId = reader.GetInt32(0);

                            var doc = new BsonDocument
                            {
                                { "_id", expenseId },
                                { "expenseId", expenseId },
                                { "reason", reader.GetString(1) },
                                { "amount", BsonDecimal128.Create(reader.GetDecimal(2)) },
                                { "date", reader.GetDateTime(3) },
                                { "category", reader.GetString(5) },
                                { "isRecurring", reader.GetBoolean(6) },
                                { "syncedAt", DateTime.UtcNow }
                            };

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", expenseId);
                            var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
                    await UpdateSyncCheckpoint(result.DeviceId, "Expenses", DateTime.UtcNow);
                }

                result.RecordCounts["Expenses"] = totalCount;
                result.Success = true;
                _logger.LogInformation("Expenses sync completed: {Count} records", totalCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncExpensesDirect: {Message}", ex.Message);
                result.RecordCounts["Expenses"] = 0;
                result.Success = false;
            }
        }

        private async Task SyncEmployeesDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting sync of employees to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("employees");

                var checkpoint = await GetSyncCheckpoint(result.DeviceId, "Employees");
                var lastSyncTime = checkpoint?.LastSyncTime ?? DateTime.UtcNow.AddDays(-3);

                int totalCount = 0;
                var bulkOps = new List<WriteModel<BsonDocument>>();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT EmployeeId, Username, PasswordHash, FirstName, LastName, Role, IsActive, CreatedAt, LastLogin, MonthlySalary, CurrentBalance
                        FROM Employees
                        WHERE e.CreatedAt > @LastSyncTime AND IsActive = 1
                        ORDER BY CreatedAt ASC";

                    command.Parameters.Add(new SqlParameter("@LastSyncTime", lastSyncTime));
                    command.CommandTimeout = 120;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            totalCount++;
                            var employeeId = reader.GetInt32(0);

                            var doc = new BsonDocument
                            {
                                { "_id", employeeId },
                                { "employeeId", employeeId },
                                { "username", reader.GetString(1) },
                                { "firstName", reader.GetString(3) },
                                { "lastName", reader.GetString(4) },
                                { "role", reader.GetString(5) },
                                { "isActive", reader.GetBoolean(6) },
                                { "monthlySalary", BsonDecimal128.Create(reader.GetDecimal(9)) },
                                { "currentBalance", BsonDecimal128.Create(reader.GetDecimal(10)) },
                                { "syncedAt", DateTime.UtcNow }
                            };

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", employeeId);
                            var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
                    await UpdateSyncCheckpoint(result.DeviceId, "Employees", DateTime.UtcNow);
                }

                result.RecordCounts["Employees"] = totalCount;
                result.Success = true;
                _logger.LogInformation("Employees sync completed: {Count} records", totalCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncEmployeesDirect: {Message}", ex.Message);
                result.RecordCounts["Employees"] = 0;
                result.Success = false;
            }
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
                _logger.LogError("Error logging sync activity: {Message}", ex.Message);
            }
        }
    }
}