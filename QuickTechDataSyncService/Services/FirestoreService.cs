using Google.Cloud.Firestore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using QuickTechDataSyncService.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using System.IO;
using TransactionModel = QuickTechDataSyncService.Models.Transaction; // Resolve naming conflict

namespace QuickTechDataSyncService.Services
{
    public class FirestoreService : IFirestoreService
    {
        private readonly ILogger<FirestoreService> _logger;
        private readonly IConfiguration _configuration;
        private FirestoreDb _firestoreDb;
        private bool _isInitialized = false;

        public FirestoreService(ILogger<FirestoreService> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        public bool IsInitialized => _isInitialized;

        public async Task<bool> InitializeAsync()
        {
            try
            {
                var configPath = _configuration["Firebase:ConfigPath"];
                _logger.LogInformation("Looking for Firebase config at: {ConfigPath}", configPath);

                // Check if the config path is specified
                if (string.IsNullOrEmpty(configPath))
                {
                    _logger.LogError("Firebase config path is not specified in appsettings.json");
                    return false;
                }

                // Check in multiple locations
                var currentDir = Directory.GetCurrentDirectory();
                var possiblePaths = new[]
                {
            configPath,                              // As specified in appsettings
            Path.Combine(currentDir, configPath),    // Relative to current directory
            Path.GetFullPath(configPath)             // Full path
        };

                string validPath = null;
                foreach (var path in possiblePaths)
                {
                    if (File.Exists(path))
                    {
                        validPath = path;
                        _logger.LogInformation("Found Firebase config at: {Path}", path);
                        break;
                    }
                }

                if (validPath == null)
                {
                    _logger.LogError("Firebase config file not found. Checked paths: {Paths}",
                        string.Join(", ", possiblePaths));
                    return false;
                }

                try
                {
                    // Verify the file is valid JSON
                    var fileContent = File.ReadAllText(validPath);
                    Newtonsoft.Json.JsonConvert.DeserializeObject(fileContent);
                    _logger.LogInformation("Firebase config file is valid JSON");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Firebase config file is not valid JSON");
                    return false;
                }

                // Set the environment variable and initialize Firestore
                Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", validPath);
                _logger.LogInformation("Set GOOGLE_APPLICATION_CREDENTIALS to: {Path}", validPath);

                _firestoreDb = await FirestoreDb.CreateAsync();
                _isInitialized = true;
                _logger.LogInformation("Firestore initialized successfully with project ID: {ProjectId}",
                    _firestoreDb.ProjectId);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize Firestore: {Message}", ex.Message);
                _isInitialized = false;
                return false;
            }
        }

        #region Products
        public async Task<bool> SyncProductsAsync(IEnumerable<Product> products)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("Firestore not initialized. Call InitializeAsync first.");
                return false;
            }

            try
            {
                var batch = _firestoreDb.StartBatch();
                var collectionRef = _firestoreDb.Collection("products");

                foreach (var product in products)
                {
                    var docRef = collectionRef.Document(product.ProductId.ToString());
                    batch.Set(docRef, ConvertProductToDocument(product));
                }

                await batch.CommitAsync();
                _logger.LogInformation("Successfully synced {Count} products to Firestore", products.Count());
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing products to Firestore");
                return false;
            }
        }

        public async Task<List<Product>> GetProductsAsync(DateTime? lastSyncTime = null)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("Firestore not initialized. Call InitializeAsync first.");
                return new List<Product>();
            }

            try
            {
                // Create a query on the products collection
                Query query = _firestoreDb.Collection("products");

                if (lastSyncTime.HasValue)
                {
                    // If we have a timestamp, only get products updated after that time
                    query = query.WhereGreaterThanOrEqualTo("updatedAt", lastSyncTime.Value);
                }

                // Execute the query
                QuerySnapshot snapshot = await query.GetSnapshotAsync();
                var products = new List<Product>();

                foreach (DocumentSnapshot document in snapshot.Documents)
                {
                    var product = ConvertDocumentToProduct(document);
                    products.Add(product);
                }

                _logger.LogInformation("Successfully retrieved {Count} products from Firestore", products.Count);
                return products;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error retrieving products from Firestore");
                return new List<Product>();
            }
        }

        private Dictionary<string, object> ConvertProductToDocument(Product product)
        {
            return new Dictionary<string, object>
            {
                { "productId", product.ProductId },
                { "name", product.Name },
                { "barcode", product.Barcode },
                { "description", product.Description ?? "" },
                { "categoryId", product.CategoryId },
                { "purchasePrice", product.PurchasePrice },
                { "salePrice", product.SalePrice },
                { "currentStock", product.CurrentStock },
                { "minimumStock", product.MinimumStock },
                { "supplierId", product.SupplierId ?? 0 },
                { "isActive", product.IsActive },
                { "createdAt", product.CreatedAt },
                { "updatedAt", product.UpdatedAt ?? Timestamp.GetCurrentTimestamp().ToDateTime() },
                { "imagePath", product.ImagePath ?? "" }
            };
        }

        private Product ConvertDocumentToProduct(DocumentSnapshot document)
        {
            var data = document.ToDictionary();

            return new Product
            {
                ProductId = Convert.ToInt32(data["productId"]),
                Name = data["name"].ToString(),
                Barcode = data["barcode"].ToString(),
                Description = data["description"].ToString(),
                CategoryId = Convert.ToInt32(data["categoryId"]),
                PurchasePrice = Convert.ToDecimal(data["purchasePrice"]),
                SalePrice = Convert.ToDecimal(data["salePrice"]),
                CurrentStock = Convert.ToDecimal(data["currentStock"]),
                MinimumStock = Convert.ToInt32(data["minimumStock"]),
                SupplierId = Convert.ToInt32(data["supplierId"]),
                IsActive = Convert.ToBoolean(data["isActive"]),
                CreatedAt = (data["createdAt"] as Timestamp?)?.ToDateTime() ?? DateTime.UtcNow,
                UpdatedAt = (data["updatedAt"] as Timestamp?)?.ToDateTime(),
                ImagePath = data["imagePath"].ToString()
            };
        }
        #endregion

        #region Categories
        public async Task<bool> SyncCategoriesAsync(IEnumerable<Category> categories)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("Firestore not initialized. Call InitializeAsync first.");
                return false;
            }

            try
            {
                var batch = _firestoreDb.StartBatch();
                var collectionRef = _firestoreDb.Collection("categories");

                foreach (var category in categories)
                {
                    var docRef = collectionRef.Document(category.CategoryId.ToString());
                    batch.Set(docRef, ConvertCategoryToDocument(category));
                }

                await batch.CommitAsync();
                _logger.LogInformation("Successfully synced {Count} categories to Firestore", categories.Count());
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing categories to Firestore");
                return false;
            }
        }

        private Dictionary<string, object> ConvertCategoryToDocument(Category category)
        {
            return new Dictionary<string, object>
            {
                { "categoryId", category.CategoryId },
                { "name", category.Name },
                { "description", category.Description ?? "" },
                { "isActive", category.IsActive },
                { "type", category.Type }
            };
        }
        #endregion

        #region Customers
        public async Task<bool> SyncCustomersAsync(IEnumerable<Customer> customers)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("Firestore not initialized. Call InitializeAsync first.");
                return false;
            }

            try
            {
                var batch = _firestoreDb.StartBatch();
                var collectionRef = _firestoreDb.Collection("customers");

                foreach (var customer in customers)
                {
                    var docRef = collectionRef.Document(customer.CustomerId.ToString());
                    batch.Set(docRef, ConvertCustomerToDocument(customer));
                }

                await batch.CommitAsync();
                _logger.LogInformation("Successfully synced {Count} customers to Firestore", customers.Count());
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing customers to Firestore");
                return false;
            }
        }

        private Dictionary<string, object> ConvertCustomerToDocument(Customer customer)
        {
            return new Dictionary<string, object>
            {
                { "customerId", customer.CustomerId },
                { "name", customer.Name },
                { "phone", customer.Phone ?? "" },
                { "email", customer.Email ?? "" },
                { "address", customer.Address ?? "" },
                { "isActive", customer.IsActive },
                { "createdAt", customer.CreatedAt },
                { "updatedAt", customer.UpdatedAt ?? Timestamp.GetCurrentTimestamp().ToDateTime() },
                { "balance", customer.Balance }
            };
        }
        #endregion

        #region Transactions
        public async Task<bool> SyncTransactionsAsync(IEnumerable<TransactionModel> transactions)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("Firestore not initialized. Call InitializeAsync first.");
                return false;
            }

            try
            {
                var batch = _firestoreDb.StartBatch();
                var collectionRef = _firestoreDb.Collection("transactions");

                foreach (var transaction in transactions)
                {
                    var docRef = collectionRef.Document(transaction.TransactionId.ToString());

                    // Convert the transaction to a document
                    var transactionData = ConvertTransactionToDocument(transaction);
                    batch.Set(docRef, transactionData);

                    // Also create transaction details as subcollection
                    if (transaction.TransactionDetails != null && transaction.TransactionDetails.Any())
                    {
                        var detailsCollectionRef = docRef.Collection("details");
                        foreach (var detail in transaction.TransactionDetails)
                        {
                            var detailDocRef = detailsCollectionRef.Document(detail.TransactionDetailId.ToString());
                            batch.Set(detailDocRef, ConvertTransactionDetailToDocument(detail));
                        }
                    }
                }

                await batch.CommitAsync();
                _logger.LogInformation("Successfully synced {Count} transactions to Firestore", transactions.Count());
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing transactions to Firestore");
                return false;
            }
        }

        private Dictionary<string, object> ConvertTransactionToDocument(TransactionModel transaction)
        {
            return new Dictionary<string, object>
            {
                { "transactionId", transaction.TransactionId },
                { "customerId", transaction.CustomerId ?? 0 },
                { "customerName", transaction.CustomerName },
                { "totalAmount", transaction.TotalAmount },
                { "paidAmount", transaction.PaidAmount },
                { "transactionDate", transaction.TransactionDate },
                { "transactionType", transaction.TransactionType.ToString() },
                { "status", transaction.Status.ToString() },
                { "paymentMethod", transaction.PaymentMethod },
                { "cashierId", transaction.CashierId },
                { "cashierName", transaction.CashierName },
                { "cashierRole", transaction.CashierRole }
            };
        }

        private Dictionary<string, object> ConvertTransactionDetailToDocument(TransactionDetail detail)
        {
            return new Dictionary<string, object>
            {
                { "transactionDetailId", detail.TransactionDetailId },
                { "transactionId", detail.TransactionId },
                { "productId", detail.ProductId },
                { "quantity", detail.Quantity },
                { "unitPrice", detail.UnitPrice },
                { "purchasePrice", detail.PurchasePrice },
                { "discount", detail.Discount },
                { "total", detail.Total }
            };
        }
        #endregion

        #region Business Settings
        public async Task<bool> SyncBusinessSettingsAsync(IEnumerable<BusinessSetting> settings)
        {
            if (!_isInitialized)
            {
                _logger.LogWarning("Firestore not initialized. Call InitializeAsync first.");
                return false;
            }

            try
            {
                var batch = _firestoreDb.StartBatch();
                var collectionRef = _firestoreDb.Collection("business_settings");

                foreach (var setting in settings)
                {
                    // Use the setting key as the document ID for easier retrieval
                    var docRef = collectionRef.Document(setting.Key);
                    batch.Set(docRef, ConvertBusinessSettingToDocument(setting));
                }

                await batch.CommitAsync();
                _logger.LogInformation("Successfully synced {Count} business settings to Firestore", settings.Count());
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing business settings to Firestore");
                return false;
            }
        }

        private Dictionary<string, object> ConvertBusinessSettingToDocument(BusinessSetting setting)
        {
            return new Dictionary<string, object>
            {
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
        }
        #endregion

        // Record sync logs in Firestore
        public async Task LogSyncToFirestore(string deviceId, string entityType, bool success, int recordCount)
        {
            if (!_isInitialized) return;

            try
            {
                var logData = new Dictionary<string, object>
                {
                    { "deviceId", deviceId },
                    { "entityType", entityType },
                    { "timestamp", Timestamp.GetCurrentTimestamp() },
                    { "success", success },
                    { "recordCount", recordCount }
                };

                await _firestoreDb.Collection("sync_logs").AddAsync(logData);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error logging sync to Firestore");
            }
        }
    }
}