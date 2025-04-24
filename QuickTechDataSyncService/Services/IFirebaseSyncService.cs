using System.Threading.Tasks;

namespace QuickTechDataSyncService.Services
{
    public interface IFirebaseSyncService
    {
        Task<bool> InitializeFirebaseAsync();
        bool IsFirebaseInitialized { get; }
        Task<SyncResult> SyncAllDataToFirebaseAsync(string deviceId);
        Task<SyncResult> SyncEntityToFirebaseAsync(string deviceId, string entityType);
    }
}