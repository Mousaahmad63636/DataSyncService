using QuickTechDataSyncService.Models;
using System.Threading.Tasks;
namespace QuickTechDataSyncService.Services
{
    public interface IMongoDbSyncService
    {
        bool IsMongoInitialized { get; }
        Task<bool> InitializeMongoAsync();
        Task<SyncResult> SyncAllDataToMongoAsync(string deviceId);
        Task<SyncResult> SyncEntityToMongoAsync(string deviceId, string entityType);
        Task<SyncResult> SyncExpensesToMongoAsync(string deviceId);
        Task<SyncResult> SyncEmployeesToMongoAsync(string deviceId);
    }
}