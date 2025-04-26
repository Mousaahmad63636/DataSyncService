using QuickTechDataSyncService.Models;
using QuickTechDataSyncService.Models.DTOs;
using System.Threading.Tasks;

namespace QuickTechDataSyncService.Services
{
    public interface IDataSyncService
    {
        Task<SyncResponseDto<Product>> SyncProductsAsync(SyncRequestDto request);
        Task<SyncResponseDto<Category>> SyncCategoriesAsync(SyncRequestDto request);
        Task<SyncResponseDto<Customer>> SyncCustomersAsync(SyncRequestDto request);
        Task<SyncResponseDto<Supplier>> SyncSuppliersAsync(SyncRequestDto request);
        Task<SyncResponseDto<Transaction>> SyncTransactionsAsync(SyncRequestDto request);
        Task<SyncResponseDto<BusinessSetting>> SyncBusinessSettingsAsync(SyncRequestDto request);
        Task<SyncResponseDto<Expense>> SyncExpensesAsync(SyncRequestDto request);
        Task<SyncResponseDto<Employee>> SyncEmployeesAsync(SyncRequestDto request);
        Task<bool> LogSyncActivityAsync(string deviceId, string entityType, bool isSuccess, string? errorMessage = null, int recordsSynced = 0);
    }
}