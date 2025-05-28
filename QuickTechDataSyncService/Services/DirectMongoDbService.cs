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
                _logger.LogInformation("Starting direct sync of employees to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("employees");
                var bulkOps = new List<WriteModel<BsonDocument>>();
                var count = 0;

                // Get all MongoDB employee IDs first
                var filter = new BsonDocument();
                var options = new FindOptions<BsonDocument> { Projection = Builders<BsonDocument>.Projection.Include("_id") };
                var mongoEmployeeIds = new HashSet<int>();
                using (var cursor = await collection.FindAsync(filter, options))
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            if (doc.Contains("_id") && doc["_id"].IsInt32)
                            {
                                mongoEmployeeIds.Add(doc["_id"].AsInt32);
                            }
                        }
                    }
                }

                // Get all SQL employee IDs
                var sqlEmployeeIds = new HashSet<int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT EmployeeId FROM Employees";
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            sqlEmployeeIds.Add(reader.GetInt32(0));
                        }
                    }
                }

                // Identify employees to delete (in MongoDB but not in SQL)
                var employeesToDelete = mongoEmployeeIds.Where(id => !sqlEmployeeIds.Contains(id)).ToList();
                if (employeesToDelete.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", employeesToDelete);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);
                    _logger.LogInformation("Deleted {Count} employees from MongoDB that were removed from SQL", deleteResult.DeletedCount);
                    result.RecordCounts["DeletedEmployees"] = (int)deleteResult.DeletedCount;
                }

                // Now sync all current employees
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                    SELECT 
                        e.EmployeeId, e.Username, e.PasswordHash, e.FirstName, e.LastName, 
                        e.Role, e.IsActive, e.CreatedAt, e.LastLogin, e.MonthlySalary, 
                        e.CurrentBalance
                    FROM 
                        Employees e";

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            count++;
                            var employeeId = reader.GetInt32(0);

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
                                { "createdAt", reader.GetDateTime(7) },
                                { "lastLogin", reader.IsDBNull(8) ? BsonNull.Value : new BsonDateTime(reader.GetDateTime(8)) },
                                { "monthlySalary", BsonDecimal128.Create(reader.GetDecimal(9)) },
                                { "currentBalance", BsonDecimal128.Create(reader.GetDecimal(10)) },
                                { "salaryTransactions", new BsonArray() }
                            };

                            // Fetch and add salary transactions
                            var salaryTransactions = await GetSalaryTransactionsDirect(connection, employeeId);
                            employeeDoc["salaryTransactions"] = salaryTransactions;

                            var upsertFilter = Builders<BsonDocument>.Filter.Eq("_id", employeeId);
                            var upsert = new ReplaceOneModel<BsonDocument>(upsertFilter, employeeDoc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    var bulkResult = await collection.BulkWriteAsync(bulkOps);
                    _logger.LogInformation("Synced {Count} employees", count);
                }
                else
                {
                    _logger.LogInformation("No employees to sync");
                }

                result.RecordCounts["Employees"] = count;
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
                _logger.LogInformation("Starting direct sync of expenses to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("expenses");
                var bulkOps = new List<WriteModel<BsonDocument>>();
                var count = 0;

                // Get all MongoDB expense IDs first
                var filter = new BsonDocument();
                var options = new FindOptions<BsonDocument> { Projection = Builders<BsonDocument>.Projection.Include("_id") };
                var mongoExpenseIds = new HashSet<int>();
                using (var cursor = await collection.FindAsync(filter, options))
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            if (doc.Contains("_id") && doc["_id"].IsInt32)
                            {
                                mongoExpenseIds.Add(doc["_id"].AsInt32);
                            }
                        }
                    }
                }

                // Get all SQL expense IDs
                var sqlExpenseIds = new HashSet<int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT ExpenseId FROM Expenses";
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            sqlExpenseIds.Add(reader.GetInt32(0));
                        }
                    }
                }

                // Identify expenses to delete (in MongoDB but not in SQL)
                var expensesToDelete = mongoExpenseIds.Where(id => !sqlExpenseIds.Contains(id)).ToList();
                if (expensesToDelete.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", expensesToDelete);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);
                    _logger.LogInformation("Deleted {Count} expenses from MongoDB that were removed from SQL", deleteResult.DeletedCount);
                    result.RecordCounts["DeletedExpenses"] = (int)deleteResult.DeletedCount;
                }

                // Now sync all current expenses
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                    SELECT 
                        ExpenseId, Reason, Amount, Date, Notes, Category, 
                        IsRecurring, CreatedAt, UpdatedAt
                    FROM 
                        Expenses";

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            count++;
                            var doc = new BsonDocument
                            {
                                { "_id", reader.GetInt32(0) },
                                { "expenseId", reader.GetInt32(0) },
                                { "reason", reader.GetString(1) },
                                { "amount", BsonDecimal128.Create(reader.GetDecimal(2)) },
                                { "date", reader.GetDateTime(3) },
                                { "notes", reader.IsDBNull(4) ? string.Empty : reader.GetString(4) },
                                { "category", reader.GetString(5) },
                                { "isRecurring", reader.GetBoolean(6) },
                                { "createdAt", reader.GetDateTime(7) },
                                { "updatedAt", reader.IsDBNull(8) ? BsonNull.Value : new BsonDateTime(reader.GetDateTime(8)) }
                            };

                            var upsertFilter = Builders<BsonDocument>.Filter.Eq("_id", reader.GetInt32(0));
                            var upsert = new ReplaceOneModel<BsonDocument>(upsertFilter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    var bulkResult = await collection.BulkWriteAsync(bulkOps);
                    _logger.LogInformation("Synced {Count} expenses", count);
                }
                else
                {
                    _logger.LogInformation("No expenses to sync");
                }

                result.RecordCounts["Expenses"] = count;
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
                result.ErrorMessage = $"{entityType} sync failed: {ex.Message}";
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

                // Get all MongoDB category IDs first
                var filter = new BsonDocument();
                var options = new FindOptions<BsonDocument> { Projection = Builders<BsonDocument>.Projection.Include("_id") };
                var mongoCategoryIds = new HashSet<int>();
                using (var cursor = await collection.FindAsync(filter, options))
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            if (doc.Contains("_id") && doc["_id"].IsInt32)
                            {
                                mongoCategoryIds.Add(doc["_id"].AsInt32);
                            }
                        }
                    }
                }

                // Get all SQL category IDs
                var sqlCategoryIds = new HashSet<int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT CategoryId FROM Categories";
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            sqlCategoryIds.Add(reader.GetInt32(0));
                        }
                    }
                }

                // Identify categories to delete (in MongoDB but not in SQL)
                var categoriesToDelete = mongoCategoryIds.Where(id => !sqlCategoryIds.Contains(id)).ToList();
                if (categoriesToDelete.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", categoriesToDelete);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);
                    _logger.LogInformation("Deleted {Count} categories from MongoDB that were removed from SQL", deleteResult.DeletedCount);
                    result.RecordCounts["DeletedCategories"] = (int)deleteResult.DeletedCount;
                }

                // Now sync all current categories
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

                            var upsertFilter = Builders<BsonDocument>.Filter.Eq("_id", reader.GetInt32(0));
                            var upsert = new ReplaceOneModel<BsonDocument>(upsertFilter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    var bulkResult = await collection.BulkWriteAsync(bulkOps);
                    _logger.LogInformation("Synced {Count} categories", count);
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
                result.Success = false;
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

                // Get all MongoDB product IDs first
                var filter = new BsonDocument();
                var options = new FindOptions<BsonDocument> { Projection = Builders<BsonDocument>.Projection.Include("_id") };
                var mongoProductIds = new HashSet<int>();
                using (var cursor = await collection.FindAsync(filter, options))
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            if (doc.Contains("_id") && doc["_id"].IsInt32)
                            {
                                mongoProductIds.Add(doc["_id"].AsInt32);
                            }
                        }
                    }
                }

                // Get all SQL product IDs
                var sqlProductIds = new HashSet<int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT ProductId FROM Products";
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            sqlProductIds.Add(reader.GetInt32(0));
                        }
                    }
                }

                // Identify products to delete (in MongoDB but not in SQL)
                var productsToDelete = mongoProductIds.Where(id => !sqlProductIds.Contains(id)).ToList();
                if (productsToDelete.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", productsToDelete);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);
                    _logger.LogInformation("Deleted {Count} products from MongoDB that were removed from SQL", deleteResult.DeletedCount);
                    result.RecordCounts["DeletedProducts"] = (int)deleteResult.DeletedCount;
                }

                // Now sync all current products
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
                                { "purchasePrice", BsonDecimal128.Create(reader.GetDecimal(5)) },
                                { "salePrice", BsonDecimal128.Create(reader.GetDecimal(6)) },
                                { "currentStock", BsonDecimal128.Create(reader.GetDecimal(7)) },
                                { "minimumStock", reader.GetInt32(8) },
                                { "supplierId", reader.IsDBNull(9) ? BsonNull.Value : new BsonInt32(reader.GetInt32(9)) },
                                { "isActive", reader.GetBoolean(10) },
                                { "createdAt", reader.GetDateTime(11).ToUniversalTime() },
                                { "speed", reader.IsDBNull(12) ? string.Empty : reader.GetString(12) },
                                { "updatedAt", reader.IsDBNull(13) ? BsonNull.Value : new BsonDateTime(reader.GetDateTime(13).ToUniversalTime()) },
                                { "imagePath", reader.IsDBNull(14) ? string.Empty : reader.GetString(14) }
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

                            var upsertFilter = Builders<BsonDocument>.Filter.Eq("_id", reader.GetInt32(0));
                            var upsert = new ReplaceOneModel<BsonDocument>(upsertFilter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    var bulkResult = await collection.BulkWriteAsync(bulkOps);
                    _logger.LogInformation("Synced {Count} products", count);
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
                result.Success = false;
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

                // Get all MongoDB customer IDs first
                var filter = new BsonDocument();
                var options = new FindOptions<BsonDocument> { Projection = Builders<BsonDocument>.Projection.Include("_id") };
                var mongoCustomerIds = new HashSet<int>();
                using (var cursor = await collection.FindAsync(filter, options))
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            if (doc.Contains("_id") && doc["_id"].IsInt32)
                            {
                                mongoCustomerIds.Add(doc["_id"].AsInt32);
                            }
                        }
                    }
                }

                // Get all SQL customer IDs
                var sqlCustomerIds = new HashSet<int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT CustomerId FROM Customers";
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            sqlCustomerIds.Add(reader.GetInt32(0));
                        }
                    }
                }

                // Identify customers to delete (in MongoDB but not in SQL)
                var customersToDelete = mongoCustomerIds.Where(id => !sqlCustomerIds.Contains(id)).ToList();
                if (customersToDelete.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", customersToDelete);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);
                    _logger.LogInformation("Deleted {Count} customers from MongoDB that were removed from SQL", deleteResult.DeletedCount);
                    result.RecordCounts["DeletedCustomers"] = (int)deleteResult.DeletedCount;
                }

                // Now sync all current customers
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
                                { "createdAt", reader.GetDateTime(6).ToUniversalTime() },
                                { "updatedAt", reader.IsDBNull(7) ? BsonNull.Value : new BsonDateTime(reader.GetDateTime(7).ToUniversalTime()) },
                                { "balance", BsonDecimal128.Create(reader.GetDecimal(8)) }
                            };

                            var upsertFilter = Builders<BsonDocument>.Filter.Eq("_id", reader.GetInt32(0));
                            var upsert = new ReplaceOneModel<BsonDocument>(upsertFilter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    var bulkResult = await collection.BulkWriteAsync(bulkOps);
                    _logger.LogInformation("Synced {Count} customers", count);
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
                result.Success = false;
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

                // Get all MongoDB business setting IDs first
                var filter = new BsonDocument();
                var options = new FindOptions<BsonDocument> { Projection = Builders<BsonDocument>.Projection.Include("_id") };
                var mongoSettingIds = new HashSet<int>();
                using (var cursor = await collection.FindAsync(filter, options))
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            if (doc.Contains("_id") && doc["_id"].IsInt32)
                            {
                                mongoSettingIds.Add(doc["_id"].AsInt32);
                            }
                        }
                    }
                }

                // Get all SQL business setting IDs
                var sqlSettingIds = new HashSet<int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT Id FROM BusinessSettings";
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            sqlSettingIds.Add(reader.GetInt32(0));
                        }
                    }
                }

                // Identify settings to delete (in MongoDB but not in SQL)
                var settingsToDelete = mongoSettingIds.Where(id => !sqlSettingIds.Contains(id)).ToList();
                if (settingsToDelete.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", settingsToDelete);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);
                    _logger.LogInformation("Deleted {Count} business settings from MongoDB that were removed from SQL", deleteResult.DeletedCount);
                    result.RecordCounts["DeletedBusinessSettings"] = (int)deleteResult.DeletedCount;
                }

                // Now sync all current business settings
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
                                { "lastModified", reader.GetDateTime(7).ToUniversalTime() },
                                { "modifiedBy", reader.IsDBNull(8) ? string.Empty : reader.GetString(8) }
                            };

                            var upsertFilter = Builders<BsonDocument>.Filter.Eq("_id", reader.GetInt32(0));
                            var upsert = new ReplaceOneModel<BsonDocument>(upsertFilter, doc) { IsUpsert = true };
                            bulkOps.Add(upsert);
                        }
                    }
                }

                if (bulkOps.Count > 0)
                {
                    var bulkResult = await collection.BulkWriteAsync(bulkOps);
                    _logger.LogInformation("Synced {Count} business settings", count);
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
                result.Success = false;
            }
        }

        private async Task SyncTransactionsDirect(SqlConnection connection, SyncResult result)
        {
            try
            {
                _logger.LogInformation("Starting direct sync of transactions to MongoDB");
                var collection = _mongoDatabase.GetCollection<BsonDocument>("transactions");
                var transactionCount = 0;

                // Get all MongoDB transaction IDs first
                var filter = new BsonDocument();
                var options = new FindOptions<BsonDocument> { Projection = Builders<BsonDocument>.Projection.Include("_id") };
                var mongoTransactionIds = new HashSet<int>();
                using (var cursor = await collection.FindAsync(filter, options))
                {
                    while (await cursor.MoveNextAsync())
                    {
                        foreach (var doc in cursor.Current)
                        {
                            if (doc.Contains("_id") && doc["_id"].IsInt32)
                            {
                                mongoTransactionIds.Add(doc["_id"].AsInt32);
                            }
                        }
                    }
                }

                // Get all SQL transaction IDs for today
                var sqlTransactionIds = new HashSet<int>();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT TransactionId FROM Transactions WHERE CONVERT(date, TransactionDate) = CONVERT(date, GETDATE())";
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            sqlTransactionIds.Add(reader.GetInt32(0));
                        }
                    }
                }

                // Identify transactions to delete (in MongoDB but not in SQL)
                var transactionsToDelete = mongoTransactionIds.Where(id => sqlTransactionIds.Contains(id) == false && mongoTransactionIds.Contains(id)).ToList();
                if (transactionsToDelete.Any())
                {
                    var deleteFilter = Builders<BsonDocument>.Filter.In("_id", transactionsToDelete);
                    var deleteResult = await collection.DeleteManyAsync(deleteFilter);
                    _logger.LogInformation("Deleted {Count} transactions from MongoDB that were removed from SQL", deleteResult.DeletedCount);
                    result.RecordCounts["DeletedTransactions"] = (int)deleteResult.DeletedCount;
                }

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
            SELECT 
                TransactionId, CustomerId, CustomerName, TotalAmount, PaidAmount, 
                TransactionDate, TransactionType, Status, PaymentMethod, 
                CashierId, CashierName, CashierRole
            FROM Transactions
            WHERE CONVERT(date, TransactionDate) = CONVERT(date, GETDATE())
            ORDER BY TransactionDate DESC";
                    command.CommandTimeout = 120;

                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            var transactionId = reader.GetInt32(0);

                            try
                            {
                                var doc = new BsonDocument
                        {
                            { "_id", transactionId },
                            { "transactionId", transactionId }
                        };

                                // customerId (nullable int)
                                if (!reader.IsDBNull(1))
                                    doc.Add("customerId", reader.GetInt32(1));
                                else
                                    doc.Add("customerId", BsonNull.Value);

                                // customerName
                                doc.Add("customerName",
                                    reader.IsDBNull(2) ? string.Empty : reader.GetString(2));

                                // amounts
                                decimal totalAmount = reader.IsDBNull(3)
                                    ? 0m
                                    : reader.GetDecimal(3);
                                doc.Add("totalAmount", BsonDecimal128.Create(totalAmount));

                                decimal paidAmount = reader.IsDBNull(4)
                                    ? 0m
                                    : reader.GetDecimal(4);
                                doc.Add("paidAmount", BsonDecimal128.Create(paidAmount));

                                // date
                                DateTime txDate = reader.IsDBNull(5)
                                    ? DateTime.UtcNow
                                    : reader.GetDateTime(5);
                                doc.Add("transactionDate", txDate.ToUniversalTime());

                                // robust int parsing helper:
                                int ParseIntField(int idx)
                                {
                                    if (reader.IsDBNull(idx)) return 0;
                                    object raw = reader.GetValue(idx);
                                    return raw switch
                                    {
                                        int i => i,
                                        long l => Convert.ToInt32(l),
                                        string s => int.TryParse(s, out var v) ? v : 0,
                                        decimal d => Convert.ToInt32(d),
                                        _ => 0
                                    };
                                }

                                // transactionType & status
                                int transactionTypeValue = ParseIntField(6);
                                int statusValue = ParseIntField(7);
                                doc.Add("transactionType", GetEnumName(typeof(TransactionType), transactionTypeValue));
                                doc.Add("status", GetEnumName(typeof(TransactionStatus), statusValue));

                                // other strings
                                doc.Add("paymentMethod", reader.IsDBNull(8) ? string.Empty : reader.GetString(8));
                                doc.Add("cashierId", reader.IsDBNull(9) ? string.Empty : reader.GetString(9));
                                doc.Add("cashierName", reader.IsDBNull(10) ? string.Empty : reader.GetString(10));
                                doc.Add("cashierRole", reader.IsDBNull(11) ? string.Empty : reader.GetString(11));

                                // details
                                var details = await GetTransactionDetailsDirect(connection, transactionId);
                                doc.Add("transactionDetails", details);
                                doc.Add("detailCount", details.Count);

                                // guard against >15 MB
                                if (doc.ToBson().Length > 15 * 1024 * 1024)
                                {
                                    int cnt = details.Count;
                                    doc["transactionDetails"] = new BsonArray();
                                    doc.Add("detailsRemovedForSize", true);
                                    doc.Add("originalDetailCount", cnt);
                                }

                                // upsert
                                var upsertFilter = Builders<BsonDocument>.Filter.Eq("_id", transactionId);
                                await collection.ReplaceOneAsync(upsertFilter, doc, new ReplaceOptions { IsUpsert = true });

                                transactionCount++;
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Error processing transaction {TransactionId}: {Message}", transactionId, ex.Message);
                            }
                        }
                    }
                }

                result.RecordCounts["Transactions"] = transactionCount;
                result.Success = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SyncTransactionsDirect: {Message}", ex.Message);
                result.RecordCounts["Transactions"] = 0;
                result.Success = false;
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