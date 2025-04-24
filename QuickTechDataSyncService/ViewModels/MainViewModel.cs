using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        private string _firestoreStatus = "Not Initialized";
        private bool _isFirestoreInitialized = false;
        private bool _isSyncing = false;
        private string _deviceId = $"Desktop-{Environment.MachineName}";

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

        public string FirestoreStatus
        {
            get => _firestoreStatus;
            set => SetProperty(ref _firestoreStatus, value);
        }

        public bool IsFirestoreInitialized
        {
            get => _isFirestoreInitialized;
            set => SetProperty(ref _isFirestoreInitialized, value);
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

        public ICommand StartServerCommand { get; }
        public ICommand StopServerCommand { get; }
        public ICommand TestConnectionCommand { get; }
        public ICommand ClearLogsCommand { get; }
        public ICommand InitializeFirestoreCommand { get; }
        public ICommand SyncAllToFirestoreCommand { get; }
        public ICommand SyncProductsToFirestoreCommand { get; }
        public ICommand SyncCategoriestoFirestoreCommand { get; }
        public ICommand SyncCustomersToFirestoreCommand { get; }
        public ICommand SyncTransactionsToFirestoreCommand { get; }
        public ICommand SyncSettingsToFirestoreCommand { get; }

        public MainViewModel()
        {
            // Build the web application host
            _host = Program.CreateHostBuilder(Array.Empty<string>()).Build();

            // Create commands
            StartServerCommand = new RelayCommand(StartServer, () => !IsServerRunning);
            StopServerCommand = new RelayCommand(StopServer, () => IsServerRunning);
            TestConnectionCommand = new RelayCommand(TestConnection);
            ClearLogsCommand = new RelayCommand(ClearLogs);

            // Firestore commands
            InitializeFirestoreCommand = new RelayCommand(InitializeFirestore, () => !IsFirestoreInitialized && !IsSyncing);
            SyncAllToFirestoreCommand = new RelayCommand(SyncAllToFirestore, () => IsFirestoreInitialized && !IsSyncing);
            SyncProductsToFirestoreCommand = new RelayCommand(() => SyncEntityToFirestore("Products"), () => IsFirestoreInitialized && !IsSyncing);
            SyncCategoriestoFirestoreCommand = new RelayCommand(() => SyncEntityToFirestore("Categories"), () => IsFirestoreInitialized && !IsSyncing);
            SyncCustomersToFirestoreCommand = new RelayCommand(() => SyncEntityToFirestore("Customers"), () => IsFirestoreInitialized && !IsSyncing);
            SyncTransactionsToFirestoreCommand = new RelayCommand(() => SyncEntityToFirestore("Transactions"), () => IsFirestoreInitialized && !IsSyncing);
            SyncSettingsToFirestoreCommand = new RelayCommand(() => SyncEntityToFirestore("Business_Settings"), () => IsFirestoreInitialized && !IsSyncing);

            // Add initial log
            AddLogMessage("Application started. Click 'Start Server' to begin serving requests.");
        }

        private async void StartServer()
        {
            try
            {
                AddLogMessage("Starting server...");
                await _host.StartAsync();

                // Get server URLs
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

        private async void InitializeFirestore()
        {
            try
            {
                AddLogMessage("Initializing Firestore...");
                var firebaseService = _host.Services.GetRequiredService<IFirebaseSyncService>();

                var success = await firebaseService.InitializeFirebaseAsync();

                if (success)
                {
                    FirestoreStatus = "Connected";
                    IsFirestoreInitialized = true;
                    AddLogMessage("Firestore initialized successfully");
                }
                else
                {
                    FirestoreStatus = "Error";
                    IsFirestoreInitialized = false;
                    AddLogMessage("Failed to initialize Firestore. Check firebase-config.json file.");
                }
            }
            catch (Exception ex)
            {
                FirestoreStatus = "Error";
                IsFirestoreInitialized = false;
                AddLogMessage($"Firestore initialization error: {ex.Message}");
            }
        }

        private async void SyncAllToFirestore()
        {
            if (IsSyncing) return;

            try
            {
                IsSyncing = true;
                AddLogMessage("Starting full sync to Firestore...");

                var firebaseService = _host.Services.GetRequiredService<IFirebaseSyncService>();
                var result = await firebaseService.SyncAllDataToFirebaseAsync(DeviceId);

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

        private async void SyncEntityToFirestore(string entityType)
        {
            if (IsSyncing) return;

            try
            {
                IsSyncing = true;
                AddLogMessage($"Starting sync of {entityType} to Firestore...");

                var firebaseService = _host.Services.GetRequiredService<IFirebaseSyncService>();
                var result = await firebaseService.SyncEntityToFirebaseAsync(DeviceId, entityType);

                if (result.Success)
                {
                    var recordCount = result.RecordCounts.ContainsKey(entityType) ? result.RecordCounts[entityType] : 0;
                    AddLogMessage($"Sync of {entityType} completed successfully in {result.Duration.TotalSeconds:F2}s. {recordCount} records synced.");
                }
                else
                {
                    AddLogMessage($"Sync of {entityType} failed: {result.ErrorMessage}");
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

                // Keep only the last 100 messages
                while (LogMessages.Count > 100)
                {
                    LogMessages.RemoveAt(LogMessages.Count - 1);
                }
            });
        }

        public async Task ShutdownAsync()
        {
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