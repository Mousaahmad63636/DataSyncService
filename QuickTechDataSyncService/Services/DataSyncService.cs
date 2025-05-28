using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using QuickTechDataSyncService.Data;
using QuickTechDataSyncService.Models;
using QuickTechDataSyncService.Models.DTOs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace QuickTechDataSyncService.Services
{
    public class DataSyncService : IDataSyncService
    {
        private readonly ApplicationDbContext _context;
        private readonly ILogger<DataSyncService> _logger;

        public DataSyncService(ApplicationDbContext context, ILogger<DataSyncService> logger)
        {
            _context = context;
            _logger = logger;
        }

        public async Task<SyncResponseDto<Product>> SyncProductsAsync(SyncRequestDto request)
        {
            return await SyncEntityAsync<Product>(request, p => p.UpdatedAt == null || p.UpdatedAt >= request.LastSyncTime,
                p => p.Include(x => x.Category));
        }

        public async Task<SyncResponseDto<Category>> SyncCategoriesAsync(SyncRequestDto request)
        {
            return await SyncEntityAsync<Category>(request, c => c.IsActive);
        }

        public async Task<SyncResponseDto<Customer>> SyncCustomersAsync(SyncRequestDto request)
        {
            return await SyncEntityAsync<Customer>(request, c => c.UpdatedAt == null || c.UpdatedAt >= request.LastSyncTime);
        }

        public async Task<SyncResponseDto<Supplier>> SyncSuppliersAsync(SyncRequestDto request)
        {
            return await SyncEntityAsync<Supplier>(request, s => s.UpdatedAt == null || s.UpdatedAt >= request.LastSyncTime);
        }

        public async Task<SyncResponseDto<Transaction>> SyncTransactionsAsync(SyncRequestDto request)
        {
            return await SyncEntityAsync<Transaction>(
                request,
                t => t.TransactionDate.Date == DateTime.Today || (request.LastSyncTime.HasValue && t.TransactionDate >= request.LastSyncTime),
                q => q.Include(t => t.TransactionDetails));
        }

        public async Task<SyncResponseDto<BusinessSetting>> SyncBusinessSettingsAsync(SyncRequestDto request)
        {
            return await SyncEntityAsync<BusinessSetting>(request, b => b.LastModified >= request.LastSyncTime);
        }
        public async Task<SyncResponseDto<Expense>> SyncExpensesAsync(SyncRequestDto request)
        {
            return await SyncEntityAsync<Expense>(request, e => e.UpdatedAt == null || e.UpdatedAt >= request.LastSyncTime);
        }
        public async Task<SyncResponseDto<Employee>> SyncEmployeesAsync(SyncRequestDto request)
        {
            return await SyncEntityAsync<Employee>(request,
            e => e.CreatedAt >= request.LastSyncTime, // Use CreatedAt instead of UpdatedAt
            e => e.Include(x => x.SalaryTransactions));
        }
        public async Task<bool> LogSyncActivityAsync(string deviceId, string entityType, bool isSuccess, string? errorMessage = null, int recordsSynced = 0)
        {
            try
            {
                var syncLog = new SyncLog
                {
                    DeviceId = deviceId,
                    EntityType = entityType,
                    LastSyncTime = DateTime.UtcNow,
                    IsSuccess = isSuccess,
                    ErrorMessage = errorMessage,
                    RecordsSynced = recordsSynced
                };

                _context.SyncLogs.Add(syncLog);
                await _context.SaveChangesAsync();
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error logging sync activity for device {DeviceId} and entity {EntityType}", deviceId, entityType);
                return false;
            }
        }

        private async Task<SyncResponseDto<T>> SyncEntityAsync<T>(
            SyncRequestDto request,
            Expression<Func<T, bool>>? filter = null,
            Func<IQueryable<T>, IQueryable<T>>? include = null) where T : class
        {
            try
            {
                // Get the base query
                IQueryable<T> query = _context.Set<T>();

                // Apply includes if provided
                if (include != null)
                {
                    query = include(query);
                }

                // Apply filter if provided
                if (filter != null)
                {
                    // If last sync time is provided, only get records updated after that time
                    if (request.LastSyncTime.HasValue)
                    {
                        query = query.Where(filter);
                    }
                }

                // Get total count
                int totalCount = await query.CountAsync();
                int pageCount = (int)Math.Ceiling(totalCount / (double)request.PageSize);

                // Apply pagination
                var data = await query
                    .Skip((request.PageNumber - 1) * request.PageSize)
                    .Take(request.PageSize)
                    .ToListAsync();

                // Log the sync activity
                await LogSyncActivityAsync(request.DeviceId, request.EntityType, true, null, data.Count);

                return new SyncResponseDto<T>
                {
                    Success = true,
                    Message = $"Successfully synced {data.Count} {typeof(T).Name} records.",
                    Data = data,
                    TotalCount = totalCount,
                    PageCount = pageCount,
                    CurrentPage = request.PageNumber,
                    SyncTime = DateTime.UtcNow
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing {EntityType} for device {DeviceId}", typeof(T).Name, request.DeviceId);

                await LogSyncActivityAsync(request.DeviceId, request.EntityType, false, ex.Message);

                return new SyncResponseDto<T>
                {
                    Success = false,
                    Message = $"Error syncing {typeof(T).Name}: {ex.Message}",
                    Data = new List<T>(),
                    TotalCount = 0,
                    PageCount = 0,
                    CurrentPage = request.PageNumber,
                    SyncTime = DateTime.UtcNow
                };
            }
        }
    }
}