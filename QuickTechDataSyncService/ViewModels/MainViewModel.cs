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
        private readonly ILogger<MainViewModel> _logger;
        private string _deviceId = $"Desktop-{Environment.MachineName}";
        private Timer _autoSyncTimer;
        private readonly TimeSpan _syncInterval = TimeSpan.FromMinutes(5);
        private bool _isAutoSyncEnabled = false;

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

        public string DeviceId
        {
            get => _deviceId;
            set => SetProperty(ref _deviceId, value);
        }

        public bool IsAutoSyncEnabled
        {
            get => _isAutoSyncEnabled;
            set => SetProperty(ref _isAutoSyncEnabled, value);
        }

        public ICommand StartServerCommand { get; }
        public ICommand StopServerCommand { get; }
        public ICommand TestConnectionCommand { get; }
        public ICommand ClearLogsCommand { get; }
        public ICommand InitializeMongoDbCommand { get; }
        public ICommand SyncAllToMongoDbCommand { get; }
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
            SyncAllToMongoDbCommand = new RelayCommand(SyncAllToMongoDb, () => !IsSyncing);
            SyncProductsToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Products"), () => !IsSyncing);
            SyncCategoriesToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Categories"), () => !IsSyncing);
            SyncCustomersToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Customers"), () => !IsSyncing);
            SyncTransactionsToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Transactions"), () => !IsSyncing);
            SyncSettingsToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Business_Settings"), () => !IsSyncing);
            SyncExpensesToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Expenses"), () => !IsSyncing);
            SyncEmployeesToMongoDbCommand = new RelayCommand(() => SyncEntityToMongoDb("Employees"), () => !IsSyncing);

            AddLogMessage("Application started. Initializing automatic synchronization...");

            _autoSyncTimer = new Timer(async _ => await RunAutoSyncAsync(), null, Timeout.Infinite, Timeout.Infinite);

            StartAutoSync();
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
            if (IsAutoSyncEnabled) return;

            AddLogMessage("Starting automatic synchronization service...");

            InitializeDatabasesAndStartSync();
        }

        private async void InitializeDatabasesAndStartSync()
        {
            try
            {
                AddLogMessage("Testing SQL Server connection...");
                await _host.StartAsync();

                var dbContext = _host.Services.GetRequiredService<Data.ApplicationDbContext>();
                var canConnect = await dbContext.Database.CanConnectAsync();

                if (!canConnect)
                {
                    AddLogMessage("ERROR: Cannot connect to SQL Server database. Auto-sync will not start.");
                    return;
                }

                ConnectionStatus = "Connected";
                AddLogMessage("SQL Server connection successful");

                AddLogMessage("Initializing MongoDB connection...");
                var mongoService = _host.Services.GetRequiredService<IMongoDbSyncService>();
                var mongoInitialized = await mongoService.InitializeMongoAsync();

                if (!mongoInitialized)
                {
                    AddLogMessage("ERROR: Cannot connect to MongoDB. Auto-sync will not start.");
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

                await RunAutoSyncAsync();

                _autoSyncTimer.Change(TimeSpan.Zero, _syncInterval);

                IsAutoSyncEnabled = true;
                AddLogMessage($"Automatic synchronization service started. Data will sync every {_syncInterval.TotalMinutes} minutes.");
            }
            catch (Exception ex)
            {
                AddLogMessage($"ERROR: Failed to initialize databases: {ex.Message}");
            }
        }

        private async Task RunAutoSyncAsync()
        {
            try
            {
                AddLogMessage("Starting scheduled data synchronization...");

                IsSyncing = true;

                var mongoService = _host.Services.GetRequiredService<IMongoDbSyncService>();

                var result = await mongoService.SyncAllDataToMongoAsync(DeviceId);

                if (result.Success)
                {
                    var summary = string.Join(", ", result.RecordCounts.Select(kv => $"{kv.Key}: {kv.Value}"));
                    AddLogMessage($"Scheduled sync completed successfully in {result.Duration.TotalSeconds:F2}s. Records: {summary}");
                }
                else
                {
                    AddLogMessage($"Scheduled sync failed: {result.ErrorMessage}");
                }

                AddLogMessage($"Next synchronization scheduled in {_syncInterval.TotalMinutes} minutes");
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
            if (IsSyncing) return;

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
            if (IsSyncing) return;

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