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

        public async Task<SyncResult> BulkSyncTransactionsAsync(string deviceId, Action<string> progressCallback = null)
        {
            var result = new SyncResult
            {
                StartTime = DateTime.UtcNow,
                DeviceId = deviceId,
                EntityType = "BulkTransactions"
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

                progressCallback?.Invoke("Getting date range for bulk sync...");

                // Get date range of transactions
                var (startDate, endDate, totalCount) = await GetTransactionDateRange(connection);

                if (totalCount == 0)
                {
                    progressCallback?.Invoke("No transactions found to sync");
                    result.Success = true;
                    result.RecordCounts["Transactions"] = 0;
                    result.EndTime = DateTime.UtcNow;
                    return result;
                }

                progressCallback?.Invoke($"Found {totalCount} transactions from {startDate:yyyy-MM-dd} to {endDate:yyyy-MM-dd}");

                var collection = _mongoDatabase.GetCollection<BsonDocument>("transactions");
                int processedCount = 0;

                // Process in weekly chunks
                var currentDate = startDate;
                while (currentDate <= endDate)
                {
                    var chunkEndDate = currentDate.AddDays(7);
                    if (chunkEndDate > endDate) chunkEndDate = endDate.AddDays(1); // Include end date

                    progressCallback?.Invoke($"Processing week: {currentDate:yyyy-MM-dd} to {chunkEndDate.AddDays(-1):yyyy-MM-dd}");

                    var chunkCount = await ProcessTransactionChunk(connection, collection, currentDate, chunkEndDate);
                    processedCount += chunkCount;

                    var progressPercent = (int)((double)processedCount / totalCount * 100);
                    progressCallback?.Invoke($"Progress: {processedCount}/{totalCount} ({progressPercent}%) - Processed {chunkCount} transactions");

                    // Update checkpoint
                    await UpdateBulkSyncCheckpoint(deviceId, currentDate, processedCount);

                    currentDate = chunkEndDate;

                    // Small delay to prevent overwhelming the system
                    await Task.Delay(1000);
                }

                // Mark bulk sync as completed
                await MarkBulkSyncCompleted(deviceId);

                result.Success = true;
                result.RecordCounts["Transactions"] = processedCount;
                result.EndTime = DateTime.UtcNow;

                progressCallback?.Invoke($"Bulk sync completed: {processedCount} transactions in {result.Duration.TotalMinutes:F1} minutes");

                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in bulk sync: {Message}", ex.Message);
                result.Success = false;
                result.ErrorMessage = ex.Message;
                result.EndTime = DateTime.UtcNow;
                progressCallback?.Invoke($"Bulk sync failed: {ex.Message}");
                return result;
            }
        }

        private async Task<(DateTime startDate, DateTime endDate, int totalCount)> GetTransactionDateRange(SqlConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"
                    SELECT 
                        MIN(TransactionDate) as StartDate,
                        MAX(TransactionDate) as EndDate,
                        COUNT(*) as TotalCount
                    FROM Transactions";

                command.CommandTimeout = 60;

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        var startDate = reader.IsDBNull(0) ? DateTime.Today : reader.GetDateTime(0).Date;
                        var endDate = reader.IsDBNull(1) ? DateTime.Today : reader.GetDateTime(1).Date;
                        var totalCount = reader.GetInt32(2);

                        return (startDate, endDate, totalCount);
                    }
                }
            }

            return (DateTime.Today, DateTime.Today, 0);
        }

        private async Task<int> ProcessTransactionChunk(SqlConnection connection, IMongoCollection<BsonDocument> collection, DateTime startDate, DateTime endDate)
        {
            var bulkOps = new List<WriteModel<BsonDocument>>();
            int chunkCount = 0;

            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"
    SELECT 
        TransactionId, CustomerId, CustomerName, TotalAmount, PaidAmount, 
        TransactionDate, TransactionType, Status
    FROM Transactions
    WHERE TransactionDate >= @StartDate
    ORDER BY TransactionDate DESC";

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        chunkCount++;
                        var transactionId = reader.GetInt32(0);

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
                            doc.Add("transactionType", reader.GetString(6));
                            doc.Add("status", reader.GetString(7));
                            doc.Add("bulkSyncedAt", DateTime.UtcNow);

                            // Get transaction details
                            var details = await GetTransactionDetailsDirect(connection, transactionId);
                            doc.Add("transactionDetails", details);

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", transactionId);
                            var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);

                            // Process in batches of 500
                            if (bulkOps.Count >= 500)
                            {
                                await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
                                bulkOps.Clear();
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error processing transaction {TransactionId} in bulk sync", transactionId);
                        }
                    }
                }
            }

            // Process remaining transactions
            if (bulkOps.Count > 0)
            {
                await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
            }

            return chunkCount;
        }

        private async Task UpdateBulkSyncCheckpoint(string deviceId, DateTime processedDate, int processedCount)
        {
            try
            {
                var checkpoint = await _dbContext.SyncCheckpoints
                    .FirstOrDefaultAsync(sc => sc.DeviceId == deviceId && sc.EntityType == "BulkTransactions");

                if (checkpoint == null)
                {
                    checkpoint = new SyncCheckpoint
                    {
                        DeviceId = deviceId,
                        EntityType = "BulkTransactions",
                        LastSyncTime = processedDate,
                        LastRecordId = processedCount,
                        CheckpointData = $"ProcessedDate:{processedDate:yyyy-MM-dd}",
                        CreatedAt = DateTime.UtcNow
                    };
                    _dbContext.SyncCheckpoints.Add(checkpoint);
                }
                else
                {
                    checkpoint.LastSyncTime = processedDate;
                    checkpoint.LastRecordId = processedCount;
                    checkpoint.CheckpointData = $"ProcessedDate:{processedDate:yyyy-MM-dd}";
                    checkpoint.UpdatedAt = DateTime.UtcNow;
                }

                await _dbContext.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating bulk sync checkpoint");
            }
        }

        private async Task MarkBulkSyncCompleted(string deviceId)
        {
            try
            {
                var checkpoint = await _dbContext.SyncCheckpoints
                    .FirstOrDefaultAsync(sc => sc.DeviceId == deviceId && sc.EntityType == "BulkTransactions");

                if (checkpoint != null)
                {
                    checkpoint.CheckpointData = "COMPLETED";
                    checkpoint.UpdatedAt = DateTime.UtcNow;
                    await _dbContext.SaveChangesAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error marking bulk sync as completed");
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

        private async Task SyncTransactionsDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting INCREMENTAL transaction sync to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("transactions");

                // Check if bulk sync is completed
                var bulkCheckpoint = await GetSyncCheckpoint(result.DeviceId, "BulkTransactions");
                var isBulkCompleted = bulkCheckpoint?.CheckpointData == "COMPLETED";

                DateTime startDate;
                if (isBulkCompleted)
                {
                    // Only sync last 3 days for incremental updates
                    startDate = DateTime.Today.AddDays(-3);
                    _logger.LogInformation("Bulk sync completed. Syncing incremental transactions from last 3 days.");
                }
                else
                {
                    // If no bulk sync, get recent transactions (last 7 days)
                    startDate = DateTime.Today.AddDays(-7);
                    _logger.LogInformation("No bulk sync detected. Syncing recent transactions from last 7 days.");
                }

                int totalCount = 0;
                var bulkOps = new List<WriteModel<BsonDocument>>();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT 
                            TransactionId, CustomerId, CustomerName, TotalAmount, PaidAmount, 
                            TransactionDate, TransactionType, Status, PaidAmount
                        FROM Transactions
                        WHERE TransactionDate >= @StartDate
                        ORDER BY TransactionDate DESC";

                    command.Parameters.Add(new SqlParameter("@StartDate", startDate));
                    command.CommandTimeout = 300;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            totalCount++;
                            var transactionId = reader.GetInt32(0);

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
                                doc.Add("transactionType", reader.GetString(6));
                                doc.Add("status", reader.GetString(7));
                                doc.Add("incrementalSyncedAt", DateTime.UtcNow);

                                var details = await GetTransactionDetailsDirect(connection, transactionId);
                                doc.Add("transactionDetails", details);

                                var filter = Builders<BsonDocument>.Filter.Eq("_id", transactionId);
                                var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                                bulkOps.Add(upsert);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Error processing transaction {TransactionId}", transactionId);
                            }
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
                    await UpdateSyncCheckpoint(result.DeviceId, "IncrementalTransactions", DateTime.UtcNow);
                }

                result.RecordCounts["Transactions"] = totalCount;
                result.Success = true;
                _logger.LogInformation("Incremental transaction sync completed. Total: {Count} synced", totalCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Critical error in SyncTransactionsDirect: {Message}", ex.Message);
                result.RecordCounts["Transactions"] = 0;
                result.Success = false;
                result.ErrorMessage = ex.Message;
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
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting transaction details for {TransactionId}: {Message}", transactionId, ex.Message);
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

                _logger.LogInformation("Starting incremental data sync to MongoDB");

                await SyncCategoriesDirect(connection, result);
                await SyncProductsDirect(connection, result);
                await SyncCustomersDirect(connection, result);
                await SyncBusinessSettingsDirect(connection, result);
                await SyncTransactionsDirect(connection, result); // Now incremental
                await SyncExpensesDirect(connection, result);
                await SyncEmployeesDirect(connection, result);

                await LogSyncActivity(deviceId, "All", true, result.RecordCounts.Values.Sum());

                result.Success = true;
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
                        WHERE ISNULL(IsActive, 1) = 1";
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
                                { "isActive", reader.IsDBNull(3) ? true : reader.GetBoolean(3) },
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
                _logger.LogInformation("Starting sync of products to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("products");

                int totalCount = 0;
                var bulkOps = new List<WriteModel<BsonDocument>>();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT 
                            p.ProductId, p.Barcode, p.Name, p.Description, p.CategoryId, 
                            p.PurchasePrice, p.SalePrice, p.CurrentStock, p.MinimumStock, 
                            p.SupplierId, p.IsActive, p.CreatedAt, p.Speed, p.UpdatedAt, p.ImagePath,
                            c.Name as CategoryName
                        FROM Products p
                        LEFT JOIN Categories c ON p.CategoryId = c.CategoryId
                        WHERE ISNULL(p.IsActive, 1) = 1
                        ORDER BY p.CreatedAt DESC";

                    command.CommandTimeout = 180;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            totalCount++;
                            var productId = reader.GetInt32(0);

                            var doc = new BsonDocument
                            {
                                { "_id", productId },
                                { "productId", productId },
                                { "barcode", reader.IsDBNull(1) ? string.Empty : reader.GetString(1) },
                                { "name", reader.GetString(2) },
                                { "description", reader.IsDBNull(3) ? string.Empty : reader.GetString(3) },
                                { "categoryId", reader.GetInt32(4) },
                                { "purchasePrice", BsonDecimal128.Create(reader.GetDecimal(5)) },
                                { "salePrice", BsonDecimal128.Create(reader.GetDecimal(6)) },
                                { "currentStock", BsonDecimal128.Create(reader.GetDecimal(7)) },
                                { "minimumStock", reader.GetInt32(8) },
                                { "isActive", reader.IsDBNull(10) ? true : reader.GetBoolean(10) },
                                { "createdAt", reader.GetDateTime(11).ToUniversalTime() },
                                { "syncedAt", DateTime.UtcNow }
                            };

                            if (!reader.IsDBNull(9))
                                doc.Add("supplierId", reader.GetInt32(9));

                            if (!reader.IsDBNull(12))
                                doc.Add("speed", reader.GetString(12));

                            if (!reader.IsDBNull(13))
                                doc.Add("updatedAt", reader.GetDateTime(13).ToUniversalTime());

                            if (!reader.IsDBNull(14))
                                doc.Add("imagePath", reader.GetString(14));

                            if (!reader.IsDBNull(15))
                            {
                                doc.Add("category", new BsonDocument
                                {
                                    { "categoryId", reader.GetInt32(4) },
                                    { "name", reader.GetString(15) }
                                });
                            }

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", productId);
                            var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
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

                int totalCount = 0;
                var bulkOps = new List<WriteModel<BsonDocument>>();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT CustomerId, Name, Phone, Email, Address, IsActive, CreatedAt, UpdatedAt, Balance
                        FROM Customers
                        WHERE ISNULL(IsActive, 1) = 1
                        ORDER BY CreatedAt DESC";

                    command.CommandTimeout = 120;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            totalCount++;
                            var customerId = reader.GetInt32(0);

                            var doc = new BsonDocument
                            {
                                { "_id", customerId },
                                { "customerId", customerId },
                                { "name", reader.GetString(1) },
                                { "phone", reader.IsDBNull(2) ? string.Empty : reader.GetString(2) },
                                { "email", reader.IsDBNull(3) ? string.Empty : reader.GetString(3) },
                                { "address", reader.IsDBNull(4) ? string.Empty : reader.GetString(4) },
                                { "isActive", reader.IsDBNull(5) ? true : reader.GetBoolean(5) },
                                { "createdAt", reader.GetDateTime(6).ToUniversalTime() },
                                { "balance", BsonDecimal128.Create(reader.GetDecimal(8)) },
                                { "syncedAt", DateTime.UtcNow }
                            };

                            if (!reader.IsDBNull(7))
                                doc.Add("updatedAt", reader.GetDateTime(7).ToUniversalTime());

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", customerId);
                            var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
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

                int totalCount = 0;
                var bulkOps = new List<WriteModel<BsonDocument>>();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT Id, [Key], Value, Description, [Group], DataType, IsSystem, LastModified, ModifiedBy
                        FROM BusinessSettings
                        ORDER BY LastModified DESC";

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
                                { "description", reader.IsDBNull(3) ? string.Empty : reader.GetString(3) },
                                { "group", reader.IsDBNull(4) ? string.Empty : reader.GetString(4) },
                                { "dataType", reader.IsDBNull(5) ? "string" : reader.GetString(5) },
                                { "isSystem", reader.IsDBNull(6) ? false : reader.GetBoolean(6) },
                                { "lastModified", reader.GetDateTime(7).ToUniversalTime() },
                                { "modifiedBy", reader.IsDBNull(8) ? string.Empty : reader.GetString(8) },
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

                int totalCount = 0;
                var bulkOps = new List<WriteModel<BsonDocument>>();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT ExpenseId, Reason, Amount, Date, Notes, Category, IsRecurring, CreatedAt, UpdatedAt
                        FROM Expenses
                        ORDER BY CreatedAt DESC";

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
                                { "date", reader.GetDateTime(3).ToUniversalTime() },
                                { "notes", reader.IsDBNull(4) ? string.Empty : reader.GetString(4) },
                                { "category", reader.GetString(5) },
                                { "isRecurring", reader.GetBoolean(6) },
                                { "createdAt", reader.GetDateTime(7).ToUniversalTime() },
                                { "syncedAt", DateTime.UtcNow }
                            };

                            if (!reader.IsDBNull(8))
                                doc.Add("updatedAt", reader.GetDateTime(8).ToUniversalTime());

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", expenseId);
                            var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
                }

                result.RecordCounts["Expenses"] = totalCount;
                result.Success = true;
                _logger.LogInformation("Expenses sync completed: {Count} records (Note: Your database has 0 expenses)", totalCount);
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

                int totalCount = 0;
                var bulkOps = new List<WriteModel<BsonDocument>>();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT EmployeeId, Username, PasswordHash, FirstName, LastName, Role, IsActive, CreatedAt
                        FROM Employees
                        WHERE ISNULL(IsActive, 1) = 1
                        ORDER BY CreatedAt DESC";

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
                                { "passwordHash", reader.GetString(2) },
                                { "firstName", reader.GetString(3) },
                                { "lastName", reader.GetString(4) },
                                { "role", reader.GetString(5) },
                                { "isActive", reader.IsDBNull(6) ? true : reader.GetBoolean(6) },
                                { "createdAt", reader.GetDateTime(7).ToUniversalTime() },
                                { "syncedAt", DateTime.UtcNow }
                            };

                            var salaryTransactions = await GetEmployeeSalaryTransactionsDirect(connection, employeeId);
                            doc.Add("salaryTransactions", salaryTransactions);

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", employeeId);
                            var upsert = new ReplaceOneModel<BsonDocument>(filter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
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

        private async Task<BsonArray> GetEmployeeSalaryTransactionsDirect(SqlConnection connection, int employeeId)
        {
            var transactions = new BsonArray();
            try
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        SELECT Id, EmployeeId, Amount, TransactionType, TransactionDate, Notes
                        FROM EmployeeSalaryTransactions
                        WHERE EmployeeId = @EmployeeId
                        ORDER BY TransactionDate DESC";

                    command.Parameters.Add(new SqlParameter("@EmployeeId", employeeId));
                    command.CommandTimeout = 60;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var transactionDoc = new BsonDocument
                            {
                                { "id", reader.GetInt32(0) },
                                { "employeeId", reader.GetInt32(1) },
                                { "amount", BsonDecimal128.Create(reader.GetDecimal(2)) },
                                { "transactionType", reader.GetString(3) },
                                { "transactionDate", reader.GetDateTime(4).ToUniversalTime() },
                                { "notes", reader.IsDBNull(5) ? string.Empty : reader.GetString(5) }
                            };

                            transactions.Add(transactionDoc);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Could not get salary transactions for employee {EmployeeId} (table may not exist): {Message}", employeeId, ex.Message);
            }

            return transactions;
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