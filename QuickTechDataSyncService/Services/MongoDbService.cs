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
using System.Text.Json;
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

            // Register global conventions
            var conventionPack = new ConventionPack
            {
                new CamelCaseElementNameConvention(),
                new IgnoreExtraElementsConvention(true)
            };
            ConventionRegistry.Register("camelCase", conventionPack, t => true);
        }

        public bool IsInitialized => _isInitialized;

        public async Task<bool> InitializeAsync()
        {
            try
            {
                string connectionString = _configuration.GetConnectionString("MongoDb");
                if (string.IsNullOrEmpty(connectionString))
                {
                    _logger.LogError("MongoDB connection string is missing in configuration");
                    return false;
                }

                var settings = MongoClientSettings.FromConnectionString(connectionString);
                settings.ServerSelectionTimeout = TimeSpan.FromSeconds(10);
                _client = new MongoClient(settings);

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


        public async Task<bool> SyncEmployeesAsync(IEnumerable<Employee> employees)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("MongoDB not initialized. Call InitializeAsync first.");
                return false;
            }
            try
            {
                _logger.LogInformation("Starting employee sync with {Count} employees", employees.Count());

                var collection = _database.GetCollection<BsonDocument>("employees");
                var bulkOperations = new List<WriteModel<BsonDocument>>();

                foreach (var employee in employees)
                {
                    try
                    {
                        var salaryTransactions = new BsonArray();
                        foreach (var txn in employee.SalaryTransactions)
                        {
                            salaryTransactions.Add(new BsonDocument
                {
                    { "id", txn.Id },
                    { "employeeId", txn.EmployeeId },
                    { "amount", txn.Amount },
                    { "transactionType", txn.TransactionType },
                    { "transactionDate", txn.TransactionDate },
                    { "notes", txn.Notes ?? string.Empty }
                });
                        }

                        var employeeDoc = new BsonDocument
            {
                { "_id", employee.EmployeeId },
                { "employeeId", employee.EmployeeId },
                { "username", employee.Username },
                { "passwordHash", employee.PasswordHash },
                { "firstName", employee.FirstName },
                { "lastName", employee.LastName },
                { "role", employee.Role },
                { "isActive", employee.IsActive },
                { "createdAt", employee.CreatedAt },
                { "lastLogin", employee.LastLogin.HasValue ? new BsonDateTime(employee.LastLogin.Value) : BsonNull.Value },
                { "monthlySalary", employee.MonthlySalary },
                { "currentBalance", employee.CurrentBalance },
                { "salaryTransactions", salaryTransactions }
            };

                        var filter = Builders<BsonDocument>.Filter.Eq("_id", employee.EmployeeId);
                        var upsert = new ReplaceOneModel<BsonDocument>(filter, employeeDoc) { IsUpsert = true };
                        bulkOperations.Add(upsert);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error preparing employee {EmployeeId} for MongoDB sync: {Message}",
                            employee.EmployeeId, ex.Message);
                    }
                }

                if (bulkOperations.Any())
                {
                    var result = await collection.BulkWriteAsync(bulkOperations);
                    _logger.LogInformation("Successfully synced {Count} employees to MongoDB", bulkOperations.Count);
                    return true;
                }
                else
                {
                    _logger.LogWarning("No employee operations to perform");
                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing employees to MongoDB: {Message}", ex.Message);
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
                var collection = _database.GetCollection<BsonDocument>("products");

                FilterDefinition<BsonDocument> filter;
                if (lastSyncTime.HasValue)
                {
                    filter = Builders<BsonDocument>.Filter.Gte("updatedAt", lastSyncTime.Value);
                }
                else
                {
                    filter = Builders<BsonDocument>.Filter.Empty;
                }

                var bsonProducts = await collection.Find(filter).ToListAsync();
                _logger.LogInformation("Successfully retrieved {Count} products from MongoDB", bsonProducts.Count);

                var products = new List<Product>();
                foreach (var bsonProduct in bsonProducts)
                {
                    try
                    {
                        var product = new Product
                        {
                            ProductId = bsonProduct["productId"].ToInt32(),
                            Barcode = bsonProduct.Contains("barcode") ? bsonProduct["barcode"].ToString() : string.Empty,
                            Name = bsonProduct.Contains("name") ? bsonProduct["name"].ToString() : string.Empty,
                            Description = bsonProduct.Contains("description") ? bsonProduct["description"].ToString() : null,
                            CategoryId = bsonProduct["categoryId"].ToInt32(),
                            PurchasePrice = bsonProduct.Contains("purchasePrice") ? bsonProduct["purchasePrice"].ToDecimal() : 0,
                            SalePrice = bsonProduct.Contains("salePrice") ? bsonProduct["salePrice"].ToDecimal() : 0,
                            CurrentStock = bsonProduct.Contains("currentStock") ? bsonProduct["currentStock"].ToDecimal() : 0,
                            MinimumStock = bsonProduct.Contains("minimumStock") ? bsonProduct["minimumStock"].ToInt32() : 0,
                            SupplierId = bsonProduct.Contains("supplierId") && !bsonProduct["supplierId"].IsBsonNull ? bsonProduct["supplierId"].ToInt32() : null,
                            IsActive = bsonProduct.Contains("isActive") ? bsonProduct["isActive"].ToBoolean() : true,
                            CreatedAt = bsonProduct.Contains("createdAt") ? bsonProduct["createdAt"].ToUniversalTime() : DateTime.UtcNow,
                            Speed = bsonProduct.Contains("speed") ? bsonProduct["speed"].ToString() : null,
                            UpdatedAt = bsonProduct.Contains("updatedAt") && !bsonProduct["updatedAt"].IsBsonNull ? bsonProduct["updatedAt"].ToUniversalTime() : null,
                            ImagePath = bsonProduct.Contains("imagePath") ? bsonProduct["imagePath"].ToString() : null
                        };

                        products.Add(product);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error converting BSON product to Product model");
                    }
                }

                return products;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving products from MongoDB: {Message}", ex.Message);
                return new List<Product>();
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
                _logger.LogInformation("Starting product sync with {Count} products", products.Count());

                var collection = _database.GetCollection<BsonDocument>("products");
                var bulkOperations = new List<WriteModel<BsonDocument>>();

                foreach (var product in products)
                {
                    try
                    {
                        // Create a simplified product document for MongoDB
                        var productDoc = new BsonDocument
                        {
                            { "_id", product.ProductId },
                            { "productId", product.ProductId },
                            { "barcode", product.Barcode ?? string.Empty },
                            { "name", product.Name },
                            { "description", product.Description ?? string.Empty },
                            { "categoryId", product.CategoryId },
                            { "purchasePrice", product.PurchasePrice },
                            { "salePrice", product.SalePrice },
                            { "currentStock", product.CurrentStock },
                            { "minimumStock", product.MinimumStock },
                            { "supplierId", product.SupplierId.HasValue ? new BsonInt32(product.SupplierId.Value) : BsonNull.Value },
                            { "isActive", product.IsActive },
                            { "createdAt", product.CreatedAt },
                            { "speed", product.Speed ?? string.Empty },
                            { "updatedAt", product.UpdatedAt.HasValue ? new BsonDateTime(product.UpdatedAt.Value) : BsonNull.Value },
                            { "imagePath", product.ImagePath ?? string.Empty }
                        };

                        // Add category information if available
                        if (product.Category != null)
                        {
                            productDoc.Add("category", new BsonDocument
                            {
                                { "categoryId", product.Category.CategoryId },
                                { "name", product.Category.Name },
                                { "description", product.Category.Description ?? string.Empty },
                                { "isActive", product.Category.IsActive },
                                { "type", product.Category.Type }
                            });
                        }

                        var filter = Builders<BsonDocument>.Filter.Eq("_id", product.ProductId);
                        var upsert = new ReplaceOneModel<BsonDocument>(filter, productDoc) { IsUpsert = true };
                        bulkOperations.Add(upsert);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error preparing product {ProductId} for MongoDB sync: {Message}",
                            product.ProductId, ex.Message);
                        // Continue with other products
                    }
                }

                if (bulkOperations.Any())
                {
                    var result = await collection.BulkWriteAsync(bulkOperations);
                    _logger.LogInformation("Successfully synced {Count} products to MongoDB. Inserted: {Inserted}, Modified: {Modified}",
                        bulkOperations.Count, result.InsertedCount, result.ModifiedCount);
                    return true;
                }
                else
                {
                    _logger.LogWarning("No product operations to perform");
                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing products to MongoDB: {Message}", ex.Message);
                return false;
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
                _logger.LogInformation("Starting category sync with {Count} categories", categories.Count());

                var collection = _database.GetCollection<BsonDocument>("categories");
                var bulkOperations = new List<WriteModel<BsonDocument>>();

                foreach (var category in categories)
                {
                    try
                    {
                        var categoryDoc = new BsonDocument
                        {
                            { "_id", category.CategoryId },
                            { "categoryId", category.CategoryId },
                            { "name", category.Name },
                            { "description", category.Description ?? string.Empty },
                            { "isActive", category.IsActive },
                            { "type", category.Type }
                        };

                        var filter = Builders<BsonDocument>.Filter.Eq("_id", category.CategoryId);
                        var upsert = new ReplaceOneModel<BsonDocument>(filter, categoryDoc) { IsUpsert = true };
                        bulkOperations.Add(upsert);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error preparing category {CategoryId} for MongoDB sync: {Message}",
                            category.CategoryId, ex.Message);
                    }
                }

                if (bulkOperations.Any())
                {
                    var result = await collection.BulkWriteAsync(bulkOperations);
                    _logger.LogInformation("Successfully synced {Count} categories to MongoDB", bulkOperations.Count);
                    return true;
                }
                else
                {
                    _logger.LogWarning("No category operations to perform");
                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing categories to MongoDB: {Message}", ex.Message);
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
                _logger.LogInformation("Starting customer sync with {Count} customers", customers.Count());

                var collection = _database.GetCollection<BsonDocument>("customers");
                var bulkOperations = new List<WriteModel<BsonDocument>>();

                foreach (var customer in customers)
                {
                    try
                    {
                        var customerDoc = new BsonDocument
                        {
                            { "_id", customer.CustomerId },
                            { "customerId", customer.CustomerId },
                            { "name", customer.Name },
                            { "phone", customer.Phone ?? string.Empty },
                            { "email", customer.Email ?? string.Empty },
                            { "address", customer.Address ?? string.Empty },
                            { "isActive", customer.IsActive },
                            { "createdAt", customer.CreatedAt },
                            { "updatedAt", customer.UpdatedAt.HasValue ? new BsonDateTime(customer.UpdatedAt.Value) : BsonNull.Value },
                            { "balance", customer.Balance }
                        };

                        var filter = Builders<BsonDocument>.Filter.Eq("_id", customer.CustomerId);
                        var upsert = new ReplaceOneModel<BsonDocument>(filter, customerDoc) { IsUpsert = true };
                        bulkOperations.Add(upsert);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error preparing customer {CustomerId} for MongoDB sync: {Message}",
                            customer.CustomerId, ex.Message);
                    }
                }

                if (bulkOperations.Any())
                {
                    var result = await collection.BulkWriteAsync(bulkOperations);
                    _logger.LogInformation("Successfully synced {Count} customers to MongoDB", bulkOperations.Count);
                    return true;
                }
                else
                {
                    _logger.LogWarning("No customer operations to perform");
                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing customers to MongoDB: {Message}", ex.Message);
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
                _logger.LogInformation("Starting transaction sync with {Count} transactions", transactions.Count());

                // Create a completely manual approach using BsonDocuments
                var collection = _database.GetCollection<BsonDocument>("transactions");

                foreach (var transaction in transactions)
                {
                    try
                    {
                        // Log transaction details for debugging
                        _logger.LogInformation("Processing transaction {Id}: Type={Type}, CustomerId={CustomerId}",
                            transaction.TransactionId,
                            transaction.TransactionType,
                            transaction.CustomerId);

                        // Create a completely manual BsonDocument
                        var doc = new BsonDocument();

                        // Add ID fields first (most likely source of error)
                        doc.Add("_id", transaction.TransactionId);
                        doc.Add("transactionId", transaction.TransactionId);

                        // Add CustomerId (handling nullable)
                        if (transaction.CustomerId.HasValue)
                        {
                            doc.Add("customerId", transaction.CustomerId.Value);
                        }
                        else
                        {
                            doc.Add("customerId", BsonNull.Value);
                        }

                        // Add scalar properties
                        doc.Add("customerName", transaction.CustomerName ?? string.Empty);
                        doc.Add("totalAmount", transaction.TotalAmount);
                        doc.Add("paidAmount", transaction.PaidAmount);
                        doc.Add("transactionDate", transaction.TransactionDate);
                        doc.Add("transactionType", transaction.TransactionType.ToString());
                        doc.Add("status", transaction.Status.ToString());
                        doc.Add("paymentMethod", transaction.PaymentMethod ?? string.Empty);
                        doc.Add("cashierId", transaction.CashierId ?? string.Empty);
                        doc.Add("cashierName", transaction.CashierName ?? string.Empty);
                        doc.Add("cashierRole", transaction.CashierRole ?? string.Empty);

                        // Handle transaction details separately and carefully
                        var details = new BsonArray();

                        if (transaction.TransactionDetails != null)
                        {
                            foreach (var detail in transaction.TransactionDetails)
                            {
                                // Log detail for debugging
                                _logger.LogInformation("Processing detail {DetailId} for transaction {Id}",
                                    detail.TransactionDetailId, transaction.TransactionId);

                                var detailDoc = new BsonDocument();
                                detailDoc.Add("transactionDetailId", detail.TransactionDetailId);
                                detailDoc.Add("transactionId", detail.TransactionId);
                                detailDoc.Add("productId", detail.ProductId);
                                detailDoc.Add("quantity", detail.Quantity);
                                detailDoc.Add("unitPrice", detail.UnitPrice);
                                detailDoc.Add("purchasePrice", detail.PurchasePrice);
                                detailDoc.Add("discount", detail.Discount);
                                detailDoc.Add("total", detail.Total);

                                details.Add(detailDoc);
                            }
                        }

                        doc.Add("transactionDetails", details);

                        // Upsert the document
                        var filter = Builders<BsonDocument>.Filter.Eq("_id", transaction.TransactionId);

                        // Log before writing to DB
                        _logger.LogInformation("Upserting transaction document: {DocSummary}",
                            doc.ToString().Substring(0, Math.Min(100, doc.ToString().Length)));

                        await collection.ReplaceOneAsync(filter, doc, new ReplaceOptions { IsUpsert = true });

                        _logger.LogInformation("Successfully synced transaction {Id}", transaction.TransactionId);
                    }
                    catch (Exception ex)
                    {
                        // Log detailed exception for this specific transaction
                        _logger.LogError(ex, "Error syncing transaction {Id}: {ErrorMessage}",
                            transaction.TransactionId, ex.Message);

                        // Try to serialize the transaction to JSON for debugging
                        try
                        {
                            var options = new JsonSerializerOptions
                            {
                                WriteIndented = true,
                                ReferenceHandler = System.Text.Json.Serialization.ReferenceHandler.IgnoreCycles
                            };
                            var json = JsonSerializer.Serialize(transaction, options);
                            _logger.LogInformation("Problematic transaction data: {TransactionJson}",
                                json.Substring(0, Math.Min(500, json.Length)));
                        }
                        catch (Exception jsonEx)
                        {
                            _logger.LogError(jsonEx, "Failed to serialize transaction for debugging");
                        }

                        // Continue with other transactions
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Critical error in SyncTransactionsAsync: {Message}", ex.Message);
                return false;
            }
        }


        public async Task<bool> SyncExpensesAsync(IEnumerable<Expense> expenses)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("MongoDB not initialized. Call InitializeAsync first.");
                return false;
            }
            try
            {
                _logger.LogInformation("Starting expense sync with {Count} expenses", expenses.Count());

                var collection = _database.GetCollection<BsonDocument>("expenses");
                var bulkOperations = new List<WriteModel<BsonDocument>>();

                foreach (var expense in expenses)
                {
                    try
                    {
                        var expenseDoc = new BsonDocument
            {
                { "_id", expense.ExpenseId },
                { "expenseId", expense.ExpenseId },
                { "reason", expense.Reason },
                { "amount", expense.Amount },
                { "date", expense.Date },
                { "notes", expense.Notes ?? string.Empty },
                { "category", expense.Category },
                { "isRecurring", expense.IsRecurring },
                { "createdAt", expense.CreatedAt },
                { "updatedAt", expense.UpdatedAt.HasValue ? new BsonDateTime(expense.UpdatedAt.Value) : BsonNull.Value }
            };

                        var filter = Builders<BsonDocument>.Filter.Eq("_id", expense.ExpenseId);
                        var upsert = new ReplaceOneModel<BsonDocument>(filter, expenseDoc) { IsUpsert = true };
                        bulkOperations.Add(upsert);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error preparing expense {ExpenseId} for MongoDB sync: {Message}",
                            expense.ExpenseId, ex.Message);
                    }
                }

                if (bulkOperations.Any())
                {
                    var result = await collection.BulkWriteAsync(bulkOperations);
                    _logger.LogInformation("Successfully synced {Count} expenses to MongoDB", bulkOperations.Count);
                    return true;
                }
                else
                {
                    _logger.LogWarning("No expense operations to perform");
                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing expenses to MongoDB: {Message}", ex.Message);
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
                _logger.LogInformation("Starting business settings sync with {Count} settings", settings.Count());

                var collection = _database.GetCollection<BsonDocument>("business_settings");
                var bulkOperations = new List<WriteModel<BsonDocument>>();

                foreach (var setting in settings)
                {
                    try
                    {
                        var settingDoc = new BsonDocument
                        {
                            { "_id", setting.Id },
                            { "id", setting.Id },
                            { "key", setting.Key },
                            { "value", setting.Value },
                            { "description", setting.Description },
                            { "group", setting.Group },
                            { "dataType", setting.DataType },
                            { "isSystem", setting.IsSystem },
                            { "lastModified", setting.LastModified },
                            { "modifiedBy", setting.ModifiedBy }
                        };

                        var filter = Builders<BsonDocument>.Filter.Eq("_id", setting.Id);
                        var upsert = new ReplaceOneModel<BsonDocument>(filter, settingDoc) { IsUpsert = true };
                        bulkOperations.Add(upsert);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error preparing business setting {Id} for MongoDB sync: {Message}",
                            setting.Id, ex.Message);
                    }
                }

                if (bulkOperations.Any())
                {
                    var result = await collection.BulkWriteAsync(bulkOperations);
                    _logger.LogInformation("Successfully synced {Count} business settings to MongoDB", settings.Count());
                    return true;
                }
                else
                {
                    _logger.LogWarning("No business setting operations to perform");
                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing business settings to MongoDB: {Message}", ex.Message);
                return false;
            }
        }
        public IMongoDatabase GetMongoDatabase()
        {
            if (!_isInitialized)
            {
                throw new InvalidOperationException("MongoDB is not initialized. Call InitializeAsync first.");
            }

            return _database;
        }
        public async Task LogSyncActivityAsync(string deviceId, string entityType, bool success, int recordCount)
        {
            if (!_isInitialized) return;

            try
            {
                var syncLog = new BsonDocument
                {
                    { "_id", ObjectId.GenerateNewId() },
                    { "deviceId", deviceId },
                    { "entityType", entityType },
                    { "lastSyncTime", DateTime.UtcNow },
                    { "isSuccess", success },
                    { "recordsSynced", recordCount },
                    { "errorMessage", string.Empty }
                };

                var collection = _database.GetCollection<BsonDocument>("sync_logs");
                await collection.InsertOneAsync(syncLog);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error logging sync activity to MongoDB");
            }
        }
    }
}