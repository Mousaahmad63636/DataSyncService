using Microsoft.EntityFrameworkCore;
using QuickTechDataSyncService.Models;

namespace QuickTechDataSyncService.Data
{
    public class ApplicationDbContext : DbContext
    {
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options)
            : base(options)
        {
        }

        public DbSet<Product> Products => Set<Product>();
        public DbSet<Category> Categories => Set<Category>();
        public DbSet<Customer> Customers => Set<Customer>();
        public DbSet<Supplier> Suppliers => Set<Supplier>();
        public DbSet<Transaction> Transactions => Set<Transaction>();
        public DbSet<TransactionDetail> TransactionDetails => Set<TransactionDetail>();
        public DbSet<BusinessSetting> BusinessSettings => Set<BusinessSetting>();
        public DbSet<SyncLog> SyncLogs => Set<SyncLog>();
        public DbSet<Expense> Expenses => Set<Expense>();
        public DbSet<Employee> Employees => Set<Employee>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            // Configure SyncLog entity
            modelBuilder.Entity<SyncLog>().HasKey(s => s.Id);
            modelBuilder.Entity<SyncLog>().Property(s => s.DeviceId).HasMaxLength(50).IsRequired();
            modelBuilder.Entity<SyncLog>().Property(s => s.EntityType).HasMaxLength(50).IsRequired();
            modelBuilder.Entity<SyncLog>().HasIndex(s => new { s.DeviceId, s.EntityType });
        }
    }
}