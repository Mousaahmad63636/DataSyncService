using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Input;
using QuickTechDataSyncService.Services;

namespace QuickTechDataSyncService.ViewModels
{
    public class MainViewModel : ViewModelBase
    {
        private readonly IHost _host;
        private string _serverStatus = "Stopped";
        private string _serverUrl = string.Empty;
        private ObservableCollection<string> _logMessages = new();
        private bool _isServerRunning = false;
        private string _connectionStatus = "Disconnected";
        private string _mongoStatus = "Not Initialized";
        private bool _isMongoInitialized = false;
        private bool _isSyncing = false;
        private bool _isBulkSyncing = false;
        private bool _isAutoSyncEnabled = false; // Default OFF
        private string _bulkSyncProgress = string.Empty;
        private readonly ILogger<MainViewModel> _logger;
        private string _deviceId = $"Desktop-{Environment.MachineName}";
        private Timer _autoSyncTimer;
        private readonly TimeSpan _syncInterval = TimeSpan.FromMinutes(2);

        public string ServerStatus
        {
            get => _serverStatus;
            set => SetProperty(ref _serverStatus, value);
        }

        public string ServerUrl
        {
            get => _serverUrl;
            set => SetProperty(ref _serverUrl, value);
        }

        public ObservableCollection<string> LogMessages
        {
            get => _logMessages;
            set => SetProperty(ref _logMessages, value);
        }

        public bool IsServerRunning
        {
            get => _isServerRunning;
            set => SetProperty(ref _isServerRunning, value);
        }

        public string ConnectionStatus
        {
            get => _connectionStatus;
            set => SetProperty(ref _connectionStatus, value);
        }

        public string MongoStatus
        {
            get => _mongoStatus;
            set => SetProperty(ref _mongoStatus, value);
        }

        public bool IsMongoInitialized
        {
            get => _isMongoInitialized;
            set => SetProperty(ref _isMongoInitialized, value);
        }

        public bool IsSyncing
        {
            get => _isSyncing;
            set => SetProperty(ref _isSyncing, value);
        }

        public bool IsBulkSyncing
        {
            get => _isBulkSyncing;
            set => SetProperty(ref _isBulkSyncing, value);
        }

        public string DeviceId
        {
            get => _deviceId;
            set => SetProperty(ref _deviceId, value);
        }

        public bool IsAutoSyncEnabled
        {
            get => _isAutoSyncEnabled;
            set
            {
                if (SetProperty(ref _isAutoSyncEnabled, value))
                {
                    if (value)
                        StartAutoSync();
                    else
                        StopAutoSync();
                }
            }
        }

        public string BulkSyncProgress
        {
            get => _bulkSyncProgress;
            set => SetProperty(ref _bulkSyncProgress, value);
        }

        public ICommand StartServerCommand { get; }
        public ICommand StopServerCommand { get; }
        public ICommand TestConnectionCommand { get; }
        public ICommand ClearLogsCommand { get; }
        public ICommand InitializeMongoDbCommand { get; }
        public ICommand SyncAllToMongoDbCommand { get; }
        public ICommand InitialBulkSyncCommand { get; }
        public ICommand SyncProductsToMongoDbCommand { get; }
        public ICommand SyncCategoriesToMongoDbCommand { get; }
        public ICommand SyncCustomersToMongoDbCommand { get; }
        public ICommand SyncTransactionsToMongoDbCommand { get; }
        public ICommand SyncSettingsToMongoDbCommand { get; }
        public ICommand SyncExpensesToMongoDbCommand { get; }
        public ICommand SyncEmployeesToMongoDbCommand { get; }

        public MainViewModel(ILogger<MainViewModel> logger = null)
        {
            _logger = logger ?? CreateDefaultLogger();
            _host = Program.CreateHostBuilder(Array.Empty<string>()).Build();

            StartServerCommand = new RelayCommand(StartServer, () => !IsServerRunning);
            StopServerCommand = new RelayCommand(StopServer, () => IsServerRunning);
            TestConnectionCommand = new RelayCommand(TestConnection);
            ClearLogsCommand = new RelayCommand(ClearLogs);

            InitializeMongoDbCommand = new RelayCommand(InitializeMongoDb);
            SyncAllToMongoDbCommand = new RelayCommand(SyncAllToMongoDb, () => !IsSyncing && !IsBulkSyncing);
            InitialBulkSyncCommand = new RelayCommand(BulkSyncTransactions, () => !IsSyncing && !IsBulkSyncing);

            SyncProductsToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Products"), () => !IsSyncing && !IsBulkSyncing);
            SyncCategoriesToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Categories"), () => !IsSyncing && !IsBulkSyncing);
            SyncCustomersToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Customers"), () => !IsSyncing && !IsBulkSyncing);
            SyncTransactionsToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Transactions"), () => !IsSyncing && !IsBulkSyncing);
            SyncSettingsToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Business_Settings"), () => !IsSyncing && !IsBulkSyncing);
            SyncExpensesToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Expenses"), () => !IsSyncing && !IsBulkSyncing);
            SyncEmployeesToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Employees"), () => !IsSyncing && !IsBulkSyncing);

            AddLogMessage("Application started. Auto-sync is disabled by default.");
            AddLogMessage("Enable auto-sync toggle to start automatic synchronization.");

            _autoSyncTimer = new Timer(async _ => await RunAutoSyncAsync(), null, Timeout.Infinite, Timeout.Infinite);

            // Initialize databases but don't start auto-sync (wait for user toggle)
            InitializeDatabasesOnly();
        }

        private ILogger<MainViewModel> CreateDefaultLogger()
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddFilter("Microsoft", LogLevel.Warning)
                    .AddFilter("System", LogLevel.Warning)
                    .AddFilter("QuickTechDataSyncService", LogLevel.Information)
                    .AddConsole();
            });

            return loggerFactory.CreateLogger<MainViewModel>();
        }

        private void StartAutoSync()
        {
            if (_autoSyncTimer == null) return;

            AddLogMessage("Auto-sync enabled. Starting automatic synchronization...");
            _autoSyncTimer.Change(TimeSpan.Zero, _syncInterval);
        }

        private void StopAutoSync()
        {
            if (_autoSyncTimer == null) return;

            AddLogMessage("Auto-sync disabled. Stopping automatic synchronization.");
            _autoSyncTimer.Change(Timeout.Infinite, Timeout.Infinite);
        }

        private async void InitializeDatabasesOnly()
        {
            try
            {
                AddLogMessage("Initializing database connections...");
                await _host.StartAsync();

                var dbContext = _host.Services.GetRequiredService<Data.ApplicationDbContext>();
                var canConnect = await dbContext.Database.CanConnectAsync();

                if (!canConnect)
                {
                    AddLogMessage("ERROR: Cannot connect to SQL Server database.");
                    return;
                }

                ConnectionStatus = "Connected";
                AddLogMessage("SQL Server connection successful");

                var mongoService = _host.Services.GetRequiredService<IMongoDbSyncService>();
                var mongoInitialized = await mongoService.InitializeMongoAsync();

                if (!mongoInitialized)
                {
                    AddLogMessage("ERROR: Cannot connect to MongoDB.");
                    return;
                }

                MongoStatus = "Connected";
                IsMongoInitialized = true;
                AddLogMessage("MongoDB connection successful");

                IsServerRunning = true;
                ServerStatus = "Running";

                var server = _host.Services.GetRequiredService<IServer>();
                var addressFeature = server.Features.Get<IServerAddressesFeature>();
                ServerUrl = addressFeature?.Addresses.FirstOrDefault() ?? "http://localhost:5000";

                AddLogMessage("System ready. Toggle auto-sync to enable automatic synchronization.");
            }
            catch (Exception ex)
            {
                AddLogMessage($"ERROR: Failed to initialize: {ex.Message}");
            }
        }

        private async Task RunAutoSyncAsync()
        {
            if (!IsAutoSyncEnabled) return;

            try
            {
                AddLogMessage("Starting scheduled incremental synchronization...");

                IsSyncing = true;

                var mongoService = _host.Services.GetRequiredService<IMongoDbSyncService>();
                var result = await mongoService.SyncAllDataToMongoAsync(DeviceId);

                if (result.Success)
                {
                    var summary = string.Join(", ", result.RecordCounts.Where(kv => kv.Value > 0).Select(kv => $"{kv.Key}: {kv.Value}"));

                    if (result.RecordCounts.Values.Sum() > 0)
                    {
                        AddLogMessage($"Incremental sync completed in {result.Duration.TotalSeconds:F2}s. Updated: {summary}");
                    }
                    else
                    {
                        AddLogMessage($"No changes detected. Sync completed in {result.Duration.TotalSeconds:F2}s");
                    }
                }
                else
                {
                    AddLogMessage($"Scheduled sync failed: {result.ErrorMessage}");
                }
            }
            catch (Exception ex)
            {
                AddLogMessage($"ERROR during scheduled sync: {ex.Message}");
            }
            finally
            {
                IsSyncing = false;
            }
        }

        private async void BulkSyncTransactions()
        {
            if (IsBulkSyncing || IsSyncing) return;

            try
            {
                IsBulkSyncing = true;
                BulkSyncProgress = "Starting bulk sync...";
                AddLogMessage("Starting initial bulk sync of all transactions...");

                var mongoService = _host.Services.GetRequiredService<IMongoDbSyncService>();

                // Use new bulk sync method
                var result = await mongoService.BulkSyncTransactionsAsync(DeviceId, progress =>
                {
                    BulkSyncProgress = progress;
                    AddLogMessage(progress);
                });

                if (result.Success)
                {
                    var totalRecords = result.RecordCounts.Values.Sum();
                    AddLogMessage($"Bulk sync completed successfully in {result.Duration.TotalMinutes:F1} minutes. Total records: {totalRecords}");
                    BulkSyncProgress = $"Completed: {totalRecords} transactions synced";
                }
                else
                {
                    AddLogMessage($"Bulk sync failed: {result.ErrorMessage}");
                    BulkSyncProgress = $"Failed: {result.ErrorMessage}";
                }
            }
            catch (Exception ex)
            {
                AddLogMessage($"Error during bulk sync: {ex.Message}");
                BulkSyncProgress = $"Error: {ex.Message}";
            }
            finally
            {
                IsBulkSyncing = false;
            }
        }

        private async void StartServer()
        {
            try
            {
                AddLogMessage("Starting server...");
                await _host.StartAsync();

                var server = _host.Services.GetRequiredService<IServer>();
                var addressFeature = server.Features.Get<IServerAddressesFeature>();
                ServerUrl = addressFeature?.Addresses.FirstOrDefault() ?? "http://localhost:5000";

                IsServerRunning = true;
                ServerStatus = "Running";
                AddLogMessage($"Server started successfully at {ServerUrl}");
            }
            catch (Exception ex)
            {
                AddLogMessage($"Error starting server: {ex.Message}");
                ServerStatus = "Error";
            }
        }

        private async void StopServer()
        {
            try
            {
                AddLogMessage("Stopping server...");
                await _host.StopAsync();
                IsServerRunning = false;
                ServerStatus = "Stopped";
                AddLogMessage("Server stopped successfully");
            }
            catch (Exception ex)
            {
                AddLogMessage($"Error stopping server: {ex.Message}");
            }
        }

        private async void TestConnection()
        {
            try
            {
                AddLogMessage("Testing connection to database...");

                var dbContext = _host.Services.GetRequiredService<Data.ApplicationDbContext>();
                var canConnect = await dbContext.Database.CanConnectAsync();

                if (canConnect)
                {
                    ConnectionStatus = "Connected";
                    AddLogMessage("Database connection successful");
                }
                else
                {
                    ConnectionStatus = "Disconnected";
                    AddLogMessage("Database connection failed");
                }
            }
            catch (Exception ex)
            {
                ConnectionStatus = "Error";
                AddLogMessage($"Database connection error: {ex.Message}");
            }
        }

        private void ClearLogs()
        {
            LogMessages.Clear();
            AddLogMessage("Logs cleared");
        }

        private async void InitializeMongoDb()
        {
            try
            {
                AddLogMessage("Initializing MongoDB...");
                var mongoService = _host.Services.GetRequiredService<IMongoDbSyncService>();

                var success = await mongoService.InitializeMongoAsync();

                if (success)
                {
                    MongoStatus = "Connected";
                    IsMongoInitialized = true;
                    AddLogMessage("MongoDB initialized successfully");

                    OnPropertyChanged(nameof(IsMongoInitialized));
                }
                else
                {
                    MongoStatus = "Error";
                    IsMongoInitialized = false;
                    AddLogMessage("Failed to initialize MongoDB. Check connection settings.");
                }
            }
            catch (Exception ex)
            {
                MongoStatus = "Error";
                IsMongoInitialized = false;
                AddLogMessage($"MongoDB initialization error: {ex.Message}");
            }
        }

        private async void SyncAllToMongoDb()
        {
            if (IsSyncing || IsBulkSyncing) return;

            try
            {
                IsSyncing = true;
                AddLogMessage("Starting full sync to MongoDB...");

                var mongoService = _host.Services.GetRequiredService<IMongoDbSyncService>();
                var result = await mongoService.SyncAllDataToMongoAsync(DeviceId);

                if (result.Success)
                {
                    var recordsText = string.Join(", ", result.RecordCounts.Select(kv => $"{kv.Key}: {kv.Value}"));
                    AddLogMessage($"Full sync completed successfully in {result.Duration.TotalSeconds:F2}s. Records: {recordsText}");
                }
                else
                {
                    AddLogMessage($"Full sync failed: {result.ErrorMessage}");
                }
            }
            catch (Exception ex)
            {
                AddLogMessage($"Error during full sync: {ex.Message}");
            }
            finally
            {
                IsSyncing = false;
            }
        }

        private async void SyncEntityToMongoDb(string entityType)
        {
            if (IsSyncing || IsBulkSyncing) return;

            try
            {
                IsSyncing = true;
                AddLogMessage($"Starting sync of {entityType} to MongoDB...");

                var mongoService = _host.Services.GetRequiredService<IMongoDbSyncService>();
                var result = await mongoService.SyncEntityToMongoAsync(DeviceId, entityType);

                if (result.Success)
                {
                    var recordCount = result.RecordCounts.ContainsKey(entityType) ? result.RecordCounts[entityType] : 0;
                    AddLogMessage($"Sync of {entityType} completed successfully in {result.Duration.TotalSeconds:F2}s. {recordCount} records synced.");
                }
                else
                {
                    string errorDetails = !string.IsNullOrEmpty(result.ErrorMessage)
                        ? result.ErrorMessage
                        : "Unknown error occurred";

                    AddLogMessage($"Sync of {entityType} failed: {errorDetails}");
                }
            }
            catch (Exception ex)
            {
                AddLogMessage($"Error during {entityType} sync: {ex.Message}");
            }
            finally
            {
                IsSyncing = false;
            }
        }

        public void AddLogMessage(string message)
        {
            Application.Current.Dispatcher.Invoke(() =>
            {
                LogMessages.Insert(0, $"[{DateTime.Now:HH:mm:ss}] {message}");

                while (LogMessages.Count > 100)
                {
                    LogMessages.RemoveAt(LogMessages.Count - 1);
                }
            });
        }

        public async Task ShutdownAsync()
        {
            IsAutoSyncEnabled = false;

            if (_autoSyncTimer != null)
            {
                _autoSyncTimer.Change(Timeout.Infinite, Timeout.Infinite);
                _autoSyncTimer.Dispose();
            }

            if (IsServerRunning)
            {
                await _host.StopAsync();
            }
            await _host.WaitForShutdownAsync();
        }
    }

    public class RelayCommand : ICommand
    {
        private readonly Action _execute;
        private readonly Func<bool>? _canExecute;

        public event EventHandler? CanExecuteChanged;

        public RelayCommand(Action execute, Func<bool>? canExecute = null)
        {
            _execute = execute ?? throw new ArgumentNullException(nameof(execute));
            _canExecute = canExecute;
        }

        public bool CanExecute(object? parameter) => _canExecute == null || _canExecute();

        public void Execute(object? parameter) => _execute();

        public void RaiseCanExecuteChanged() => CanExecuteChanged?.Invoke(this, EventArgs.Empty);
    }
}