using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using QuickTechDataSyncService.Models.DTOs;
using QuickTechDataSyncService.Services;

namespace QuickTechDataSyncService.API
{
    [ApiController]
    [Route("api/sync")]
    public class SyncController : ControllerBase
    {
        private readonly IDataSyncService _dataSyncService;
        private readonly ILogger<SyncController> _logger;

        public SyncController(IDataSyncService dataSyncService, ILogger<SyncController> logger)
        {
            _dataSyncService = dataSyncService;
            _logger = logger;
        }

        [HttpPost("products")]
        public async Task<IActionResult> SyncProducts([FromBody] SyncRequestDto request)
        {
            _logger.LogInformation("Product sync requested by device {DeviceId}", request.DeviceId);
            var result = await _dataSyncService.SyncProductsAsync(request);
            return Ok(result);
        }

        [HttpPost("categories")]
        public async Task<IActionResult> SyncCategories([FromBody] SyncRequestDto request)
        {
            _logger.LogInformation("Category sync requested by device {DeviceId}", request.DeviceId);
            var result = await _dataSyncService.SyncCategoriesAsync(request);
            return Ok(result);
        }

        [HttpPost("customers")]
        public async Task<IActionResult> SyncCustomers([FromBody] SyncRequestDto request)
        {
            _logger.LogInformation("Customer sync requested by device {DeviceId}", request.DeviceId);
            var result = await _dataSyncService.SyncCustomersAsync(request);
            return Ok(result);
        }

        [HttpPost("suppliers")]
        public async Task<IActionResult> SyncSuppliers([FromBody] SyncRequestDto request)
        {
            _logger.LogInformation("Supplier sync requested by device {DeviceId}", request.DeviceId);
            var result = await _dataSyncService.SyncSuppliersAsync(request);
            return Ok(result);
        }

        [HttpPost("transactions")]
        public async Task<IActionResult> SyncTransactions([FromBody] SyncRequestDto request)
        {
            _logger.LogInformation("Transaction sync requested by device {DeviceId}", request.DeviceId);
            var result = await _dataSyncService.SyncTransactionsAsync(request);
            return Ok(result);
        }

        [HttpPost("business-settings")]
        public async Task<IActionResult> SyncBusinessSettings([FromBody] SyncRequestDto request)
        {
            _logger.LogInformation("Business settings sync requested by device {DeviceId}", request.DeviceId);
            var result = await _dataSyncService.SyncBusinessSettingsAsync(request);
            return Ok(result);
        }

        [HttpGet("status")]
        public IActionResult GetStatus()
        {
            return Ok(new { Status = "Online", Timestamp = DateTime.UtcNow });
        }


        [HttpPost("expenses")]
        public async Task<IActionResult> SyncExpenses([FromBody] SyncRequestDto request)
        {
            _logger.LogInformation("Expense sync requested by device {DeviceId}", request.DeviceId);
            var result = await _dataSyncService.SyncExpensesAsync(request);
            return Ok(result);
        }
        [HttpPost("employees")]
        public async Task<IActionResult> SyncEmployees([FromBody] SyncRequestDto request)
        {
            _logger.LogInformation("Employee sync requested by device {DeviceId}", request.DeviceId);
            var result = await _dataSyncService.SyncEmployeesAsync(request);
            return Ok(result);
        }
    }
}