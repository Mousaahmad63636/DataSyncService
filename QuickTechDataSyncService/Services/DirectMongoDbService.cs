using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using QuickTechDataSyncService.Data;
using QuickTechDataSyncService.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
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

                _logger.LogInformation("Initializing MongoDB connection");
                var settings = MongoClientSettings.FromConnectionString(connectionString);
                settings.ServerSelectionTimeout = TimeSpan.FromSeconds(10);
                _mongoClient = new MongoClient(settings);

                _mongoDatabase = _mongoClient.GetDatabase(_databaseName);

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

        private async Task UpdateSyncCheckpoint(string deviceId, string entityType, DateTime syncTime, int? lastRecordId = null)
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
                        CreatedAt = DateTime.UtcNow
                    };
                    _dbContext.SyncCheckpoints.Add(checkpoint);
                }
                else
                {
                    checkpoint.LastSyncTime = syncTime;
                    if (lastRecordId.HasValue)
                        checkpoint.LastRecordId = lastRecordId.Value;
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

                    _logger.LogInformation("Deleted {Count} transactions from MongoDB", deleteResult.DeletedCount);
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

        private async Task SyncEmployeesDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting incremental sync of employees to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("employees");

                var checkpoint = await GetSyncCheckpoint(result.DeviceId, "Employees");
                var lastSyncTime = checkpoint?.LastSyncTime ?? DateTime.UtcNow.AddDays(-30);

                const int batchSize = 500;
                int totalCount = 0;
                DateTime maxModifiedDate = lastSyncTime;
                bool hasMoreData = true;

                var mongoEmployeeIds = new HashSet<int>();
                using (var cursor = await collection.Find(new BsonDocument()).Project(Builders<BsonDocument>.Projection.Include("_id")).ToCursorAsync())
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            if (doc.Contains("_id") && doc["_id"].IsInt32)
                                mongoEmployeeIds.Add(doc["_id"].AsInt32);
                        }
                    }
                }

                var sqlEmployeeIds = new HashSet<int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT EmployeeId FROM Employees WHERE IsActive = 1";
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                            sqlEmployeeIds.Add(reader.GetInt32(0));
                    }
                }

                var employeesToDelete = mongoEmployeeIds.Where(id => !sqlEmployeeIds.Contains(id)).ToList();
                if (employeesToDelete.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", employeesToDelete);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);
                    result.RecordCounts["DeletedEmployees"] = (int)deleteResult.DeletedCount;
                }

                while (hasMoreData)
                {
                    var bulkOps = new List<WriteModel<BsonDocument>>();

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"
                    SELECT TOP (@BatchSize)
                        e.EmployeeId, e.Username, e.PasswordHash, e.FirstName, e.LastName, 
                        e.Role, e.IsActive, e.CreatedAt, e.LastLogin, e.MonthlySalary, 
                        e.CurrentBalance
                    FROM Employees e
                    WHERE e.CreatedAt > @LastSyncTime AND e.IsActive = 1
                    ORDER BY e.CreatedAt ASC, e.EmployeeId ASC";

                        command.Parameters.Add(new SqlParameter("@BatchSize", batchSize));
                        command.Parameters.Add(new SqlParameter("@LastSyncTime", lastSyncTime));
                        command.CommandTimeout = 120;

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            int batchCount = 0;

                            while (await reader.ReadAsync())
                            {
                                batchCount++;
                                var employeeId = reader.GetInt32(0);
                                var createdAt = reader.GetDateTime(7);

                                if (createdAt > maxModifiedDate)
                                    maxModifiedDate = createdAt;

                                var employeeDoc = new BsonDocument
                        {
                            { "_id", employeeId },
                            { "employeeId", employeeId },
                            { "username", reader.GetString(1) },
                            { "passwordHash", reader.GetString(2) },
                            { "firstName", reader.GetString(3) },
                            { "lastName", reader.GetString(4) },
                            { "role", reader.GetString(5) },
                            { "isActive", reader.GetBoolean(6) },
                            { "createdAt", createdAt },
                            { "lastLogin", reader.IsDBNull(8) ? BsonNull.Value : new BsonDateTime(reader.GetDateTime(8)) },
                            { "monthlySalary", BsonDecimal128.Create(reader.GetDecimal(9)) },
                            { "currentBalance", BsonDecimal128.Create(reader.GetDecimal(10)) },
                            { "syncedAt", DateTime.UtcNow }
                        };

                                var salaryTransactions = await GetSalaryTransactionsDirect(connection, employeeId);
                                employeeDoc["salaryTransactions"] = salaryTransactions;

                                var filter = Builders<BsonDocument>.Filter.Eq("_id", employeeId);
                                var upsert = new ReplaceOneModel<BsonDocument>(filter, employeeDoc) { IsUpsert = true };
                                bulkOps.Add(upsert);
                            }

                            hasMoreData = batchCount == batchSize;
                        }
                    }

                    if (bulkOps.Count > 0)
                    {
                        await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
                        totalCount += bulkOps.Count;
                        await UpdateSyncCheckpoint(result.DeviceId, "Employees", maxModifiedDate);

                        if (hasMoreData)
                            await Task.Delay(50);
                    }
                }

                result.RecordCounts["Employees"] = totalCount;
                result.Success = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncEmployeesDirect: {Message}", ex.Message);
                result.RecordCounts["Employees"] = 0;
                result.Success = false;
            }
        }
        private async Task<BsonArray> GetSalaryTransactionsDirect(SqlConnection connection, int employeeId)
        {
            var transactions = new BsonArray();
            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"
                SELECT 
                    Id, EmployeeId, Amount, TransactionType, TransactionDate, Notes
                FROM 
                    EmployeeSalaryTransactions
                WHERE 
                    EmployeeId = @EmployeeId";

                var param = command.CreateParameter();
                param.ParameterName = "@EmployeeId";
                param.Value = employeeId;
                command.Parameters.Add(param);

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        var txnDoc = new BsonDocument
                        {
                            { "id", reader.GetInt32(0) },
                            { "employeeId", reader.GetInt32(1) },
                            { "amount", BsonDecimal128.Create(reader.GetDecimal(2)) },
                            { "transactionType", reader.GetString(3) },
                            { "transactionDate", reader.GetDateTime(4) },
                            { "notes", reader.IsDBNull(5) ? string.Empty : reader.GetString(5) }
                        };

                        transactions.Add(txnDoc);
                    }
                }
            }

            return transactions;
        }

        private async Task SyncExpensesDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting incremental sync of expenses to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("expenses");

                var checkpoint = await GetSyncCheckpoint(result.DeviceId, "Expenses");
                var lastSyncTime = checkpoint?.LastSyncTime ?? DateTime.UtcNow.AddDays(-30);

                const int batchSize = 1000;
                int totalCount = 0;
                DateTime maxModifiedDate = lastSyncTime;
                bool hasMoreData = true;

                var mongoExpenseIds = new HashSet<int>();
                using (var cursor = await collection.Find(new BsonDocument()).Project(Builders<BsonDocument>.Projection.Include("_id")).ToCursorAsync())
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            if (doc.Contains("_id") && doc["_id"].IsInt32)
                                mongoExpenseIds.Add(doc["_id"].AsInt32);
                        }
                    }
                }

                var sqlExpenseIds = new HashSet<int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT ExpenseId FROM Expenses";
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                            sqlExpenseIds.Add(reader.GetInt32(0));
                    }
                }

                var expensesToDelete = mongoExpenseIds.Where(id => !sqlExpenseIds.Contains(id)).ToList();
                if (expensesToDelete.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", expensesToDelete);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);
                    result.RecordCounts["DeletedExpenses"] = (int)deleteResult.DeletedCount;
                }

                while (hasMoreData)
                {
                    var bulkOps = new List<WriteModel<BsonDocument>>();

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"
                    SELECT TOP (@BatchSize)
                        ExpenseId, Reason, Amount, Date, Notes, Category, 
                        IsRecurring, CreatedAt, UpdatedAt
                    FROM Expenses
                    WHERE UpdatedAt > @LastSyncTime OR UpdatedAt IS NULL
                    ORDER BY ISNULL(UpdatedAt, CreatedAt) ASC, ExpenseId ASC";

                        command.Parameters.Add(new SqlParameter("@BatchSize", batchSize));
                        command.Parameters.Add(new SqlParameter("@LastSyncTime", lastSyncTime));
                        command.CommandTimeout = 120;

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            int batchCount = 0;

                            while (await reader.ReadAsync())
                            {
                                batchCount++;
                                var expenseId = reader.GetInt32(0);
                                var updatedAt = reader.IsDBNull(8) ? reader.GetDateTime(7) : reader.GetDateTime(8);

                                if (updatedAt > maxModifiedDate)
                                    maxModifiedDate = updatedAt;

                                var doc = new BsonDocument
                        {
                            { "_id", expenseId },
                            { "expenseId", expenseId },
                            { "reason", reader.GetString(1) },
                            { "amount", BsonDecimal128.Create(reader.GetDecimal(2)) },
                            { "date", reader.GetDateTime(3) },
                            { "notes", reader.IsDBNull(4) ? string.Empty : reader.GetString(4) },
                            { "category", reader.GetString(5) },
                            { "isRecurring", reader.GetBoolean(6) },
                            { "createdAt", reader.GetDateTime(7) },
                            { "updatedAt", reader.IsDBNull(8) ? BsonNull.Value : new BsonDateTime(reader.GetDateTime(8)) },
                            { "syncedAt", DateTime.UtcNow }
                        };

                                var filter = Builders<BsonDocument>.Filter.Eq("_id", expenseId);
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
                        await UpdateSyncCheckpoint(result.DeviceId, "Expenses", maxModifiedDate);

                        if (hasMoreData)
                            await Task.Delay(50);
                    }
                }

                result.RecordCounts["Expenses"] = totalCount;
                result.Success = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncExpensesDirect: {Message}", ex.Message);
                result.RecordCounts["Expenses"] = 0;
                result.Success = false;
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
                _logger.LogInformation("Starting incremental sync of categories to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("categories");

                var checkpoint = await GetSyncCheckpoint(result.DeviceId, "Categories");
                var lastSyncTime = checkpoint?.LastSyncTime ?? DateTime.UtcNow.AddDays(-30);

                int totalCount = 0;
                DateTime maxModifiedDate = lastSyncTime;

                var mongoCategoryIds = new HashSet<int>();
                using (var cursor = await collection.Find(new BsonDocument()).Project(Builders<BsonDocument>.Projection.Include("_id")).ToCursorAsync())
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            if (doc.Contains("_id") && doc["_id"].IsInt32)
                                mongoCategoryIds.Add(doc["_id"].AsInt32);
                        }
                    }
                }

                var sqlCategoryIds = new HashSet<int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT CategoryId FROM Categories WHERE IsActive = 1";
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                            sqlCategoryIds.Add(reader.GetInt32(0));
                    }
                }

                var categoriesToDelete = mongoCategoryIds.Where(id => !sqlCategoryIds.Contains(id)).ToList();
                if (categoriesToDelete.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", categoriesToDelete);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);
                    result.RecordCounts["DeletedCategories"] = (int)deleteResult.DeletedCount;
                }

                var bulkOps = new List<WriteModel<BsonDocument>>();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                SELECT CategoryId, Name, Description, IsActive, Type
                FROM Categories
                WHERE IsActive = 1";

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
                _logger.LogInformation("Starting incremental sync of products to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("products");

                var checkpoint = await GetSyncCheckpoint(result.DeviceId, "Products");
                var lastSyncTime = checkpoint?.LastSyncTime ?? DateTime.UtcNow.AddDays(-30);

                const int batchSize = 1000;
                int totalCount = 0;
                DateTime maxModifiedDate = lastSyncTime;
                bool hasMoreData = true;

                var mongoProductIds = new HashSet<int>();
                using (var cursor = await collection.Find(new BsonDocument()).Project(Builders<BsonDocument>.Projection.Include("_id")).ToCursorAsync())
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            if (doc.Contains("_id") && doc["_id"].IsInt32)
                                mongoProductIds.Add(doc["_id"].AsInt32);
                        }
                    }
                }

                var sqlProductIds = new HashSet<int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT ProductId FROM Products WHERE IsActive = 1";
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                            sqlProductIds.Add(reader.GetInt32(0));
                    }
                }

                var productsToDelete = mongoProductIds.Where(id => !sqlProductIds.Contains(id)).ToList();
                if (productsToDelete.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", productsToDelete);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);
                    result.RecordCounts["DeletedProducts"] = (int)deleteResult.DeletedCount;
                }

                while (hasMoreData)
                {
                    var bulkOps = new List<WriteModel<BsonDocument>>();

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"
                    SELECT TOP (@BatchSize)
                        p.ProductId, p.Barcode, p.Name, p.Description, p.CategoryId, 
                        p.PurchasePrice, p.SalePrice, p.CurrentStock, p.MinimumStock, 
                        p.SupplierId, p.IsActive, p.CreatedAt, p.Speed, p.UpdatedAt, p.ImagePath,
                        c.Name as CategoryName, c.Description as CategoryDescription, c.IsActive as CategoryIsActive, c.Type as CategoryType
                    FROM Products p
                    LEFT JOIN Categories c ON p.CategoryId = c.CategoryId
                    WHERE (p.UpdatedAt > @LastSyncTime OR p.UpdatedAt IS NULL) AND p.IsActive = 1
                    ORDER BY ISNULL(p.UpdatedAt, p.CreatedAt) ASC, p.ProductId ASC";

                        command.Parameters.Add(new SqlParameter("@BatchSize", batchSize));
                        command.Parameters.Add(new SqlParameter("@LastSyncTime", lastSyncTime));
                        command.CommandTimeout = 120;

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
                            { "description", reader.IsDBNull(3) ? string.Empty : reader.GetString(3) },
                            { "categoryId", reader.GetInt32(4) },
                            { "purchasePrice", BsonDecimal128.Create(reader.GetDecimal(5)) },
                            { "salePrice", BsonDecimal128.Create(reader.GetDecimal(6)) },
                            { "currentStock", BsonDecimal128.Create(reader.GetDecimal(7)) },
                            { "minimumStock", reader.GetInt32(8) },
                            { "supplierId", reader.IsDBNull(9) ? BsonNull.Value : new BsonInt32(reader.GetInt32(9)) },
                            { "isActive", reader.GetBoolean(10) },
                            { "createdAt", reader.GetDateTime(11).ToUniversalTime() },
                            { "speed", reader.IsDBNull(12) ? string.Empty : reader.GetString(12) },
                            { "updatedAt", reader.IsDBNull(13) ? BsonNull.Value : new BsonDateTime(reader.GetDateTime(13).ToUniversalTime()) },
                            { "imagePath", reader.IsDBNull(14) ? string.Empty : reader.GetString(14) },
                            { "syncedAt", DateTime.UtcNow }
                        };

                                if (!reader.IsDBNull(15))
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

                        if (hasMoreData)
                            await Task.Delay(50);
                    }
                }

                result.RecordCounts["Products"] = totalCount;
                result.Success = true;
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
                _logger.LogInformation("Starting incremental sync of customers to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("customers");

                var checkpoint = await GetSyncCheckpoint(result.DeviceId, "Customers");
                var lastSyncTime = checkpoint?.LastSyncTime ?? DateTime.UtcNow.AddDays(-30);

                const int batchSize = 1000;
                int totalCount = 0;
                DateTime maxModifiedDate = lastSyncTime;
                bool hasMoreData = true;

                var mongoCustomerIds = new HashSet<int>();
                using (var cursor = await collection.Find(new BsonDocument()).Project(Builders<BsonDocument>.Projection.Include("_id")).ToCursorAsync())
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            if (doc.Contains("_id") && doc["_id"].IsInt32)
                                mongoCustomerIds.Add(doc["_id"].AsInt32);
                        }
                    }
                }

                var sqlCustomerIds = new HashSet<int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT CustomerId FROM Customers WHERE IsActive = 1";
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                            sqlCustomerIds.Add(reader.GetInt32(0));
                    }
                }

                var customersToDelete = mongoCustomerIds.Where(id => !sqlCustomerIds.Contains(id)).ToList();
                if (customersToDelete.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", customersToDelete);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);
                    result.RecordCounts["DeletedCustomers"] = (int)deleteResult.DeletedCount;
                }

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
                            { "address", reader.IsDBNull(4) ? string.Empty : reader.GetString(4) },
                            { "isActive", reader.GetBoolean(5) },
                            { "createdAt", reader.GetDateTime(6).ToUniversalTime() },
                            { "updatedAt", reader.IsDBNull(7) ? BsonNull.Value : new BsonDateTime(reader.GetDateTime(7).ToUniversalTime()) },
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
                            await Task.Delay(50);
                    }
                }

                result.RecordCounts["Customers"] = totalCount;
                result.Success = true;
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
                _logger.LogInformation("Starting incremental sync of business settings to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("business_settings");

                var checkpoint = await GetSyncCheckpoint(result.DeviceId, "BusinessSettings");
                var lastSyncTime = checkpoint?.LastSyncTime ?? DateTime.UtcNow.AddDays(-30);

                int totalCount = 0;
                DateTime maxModifiedDate = lastSyncTime;
                bool hasChanges = false;

                var mongoSettingIds = new HashSet<int>();
                using (var cursor = await collection.Find(new BsonDocument()).Project(Builders<BsonDocument>.Projection.Include("_id")).ToCursorAsync())
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            if (doc.Contains("_id") && doc["_id"].IsInt32)
                                mongoSettingIds.Add(doc["_id"].AsInt32);
                        }
                    }
                }

                var sqlSettingIds = new HashSet<int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT Id FROM BusinessSettings";
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                            sqlSettingIds.Add(reader.GetInt32(0));
                    }
                }

                var settingsToDelete = mongoSettingIds.Where(id => !sqlSettingIds.Contains(id)).ToList();
                if (settingsToDelete.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", settingsToDelete);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);
                    result.RecordCounts["DeletedBusinessSettings"] = (int)deleteResult.DeletedCount;
                    hasChanges = true;
                }

                var bulkOps = new List<WriteModel<BsonDocument>>();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                SELECT Id, [Key], Value, Description, [Group], DataType, IsSystem, LastModified, ModifiedBy
                FROM BusinessSettings
                WHERE LastModified > @LastSyncTime
                ORDER BY LastModified ASC";

                    command.Parameters.Add(new SqlParameter("@LastSyncTime", lastSyncTime));

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            totalCount++;
                            hasChanges = true;
                            var settingId = reader.GetInt32(0);
                            var lastModified = reader.GetDateTime(7);

                            if (lastModified > maxModifiedDate)
                                maxModifiedDate = lastModified;

                            var doc = new BsonDocument
                    {
                        { "_id", settingId },
                        { "id", settingId },
                        { "key", reader.GetString(1) },
                        { "value", reader.GetString(2) },
                        { "description", reader.IsDBNull(3) ? string.Empty : reader.GetString(3) },
                        { "group", reader.IsDBNull(4) ? string.Empty : reader.GetString(4) },
                        { "dataType", reader.IsDBNull(5) ? "string" : reader.GetString(5) },
                        { "isSystem", reader.GetBoolean(6) },
                        { "lastModified", lastModified.ToUniversalTime() },
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

                if (hasChanges)
                {
                    await UpdateSyncCheckpoint(result.DeviceId, "BusinessSettings", maxModifiedDate > lastSyncTime ? maxModifiedDate : DateTime.UtcNow);
                }

                result.RecordCounts["BusinessSettings"] = totalCount;
                result.Success = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncBusinessSettingsDirect: {Message}", ex.Message);
                result.RecordCounts["BusinessSettings"] = 0;
                result.Success = false;
            }
        }
        private async Task SyncTransactionsDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting incremental sync of transactions to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("transactions");

                var checkpoint = await GetSyncCheckpoint(result.DeviceId, "Transactions");
                var lastSyncTime = checkpoint?.LastSyncTime ?? DateTime.UtcNow.AddDays(-30);

                const int batchSize = 500;
                int totalCount = 0;
                int deletedCount = 0;
                DateTime maxModifiedDate = lastSyncTime;
                bool hasMoreData = true;

                if (checkpoint != null)
                {
                    deletedCount = await SyncDeletedTransactions(connection, collection, lastSyncTime);
                    result.RecordCounts["DeletedTransactions"] = deletedCount;
                }

                while (hasMoreData)
                {
                    var bulkOps = new List<WriteModel<BsonDocument>>();

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
                        command.CommandTimeout = 120;

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
                                    _logger.LogError(ex, "Error processing transaction {TransactionId}", transactionId);
                                }
                            }

                            hasMoreData = batchCount == batchSize;
                        }
                    }

                    if (bulkOps.Count > 0)
                    {
                        var bulkResult = await collection.BulkWriteAsync(bulkOps, new BulkWriteOptions { IsOrdered = false });
                        totalCount += bulkOps.Count;

                        _logger.LogInformation("Synced batch of {Count} transactions. Total so far: {Total}",
                            bulkOps.Count, totalCount);

                        await UpdateSyncCheckpoint(result.DeviceId, "Transactions", maxModifiedDate);

                        if (hasMoreData)
                            await Task.Delay(100);
                    }
                }

                _logger.LogInformation("Incremental sync completed. Synced: {Count}, Deleted: {Deleted}",
                    totalCount, deletedCount);

                result.RecordCounts["Transactions"] = totalCount;
                result.Success = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncTransactionsDirect: {Message}", ex.Message);
                result.RecordCounts["Transactions"] = 0;
                result.Success = false;
                result.ErrorMessage = ex.Message;
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
                    command.CommandTimeout = 60;

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