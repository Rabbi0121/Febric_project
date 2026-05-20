-- Fabric Warehouse star schema for Gold layer reporting

CREATE TABLE IF NOT EXISTS dbo.DimDate (
    DateKey INT NOT NULL PRIMARY KEY,
    DateValue DATE NOT NULL,
    [Year] INT NOT NULL,
    [Month] INT NOT NULL,
    [Day] INT NOT NULL,
    DayName NVARCHAR(20) NOT NULL,
    MonthName NVARCHAR(20) NOT NULL,
    IsWeekend BIT NOT NULL
);

CREATE TABLE IF NOT EXISTS dbo.DimZone (
    ZoneKey INT NOT NULL PRIMARY KEY,
    ZoneId INT NULL,
    ZoneName NVARCHAR(255) NULL,
    Borough NVARCHAR(100) NULL,
    City NVARCHAR(100) NULL,
    CountryCode NVARCHAR(10) NULL
);

CREATE TABLE IF NOT EXISTS dbo.DimFX (
    FXKey BIGINT NOT NULL PRIMARY KEY,
    FXDate DATE NOT NULL,
    USDToEURRate DECIMAL(18, 6) NOT NULL
);

CREATE TABLE IF NOT EXISTS dbo.DimGDP (
    GDPKey BIGINT NOT NULL PRIMARY KEY,
    CountryCode NVARCHAR(10) NOT NULL,
    [Year] INT NOT NULL,
    GDPUsd DECIMAL(38, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS dbo.FactTaxiDaily (
    FactTaxiDailyKey BIGINT NOT NULL PRIMARY KEY,
    DateKey INT NOT NULL,
    ZoneKey INT NULL,
    TripCount BIGINT NOT NULL,
    PassengerTotal BIGINT NOT NULL,
    RevenueUSD DECIMAL(18, 2) NOT NULL,
    AvgTripDistanceMiles DECIMAL(18, 4) NULL
);

CREATE TABLE IF NOT EXISTS dbo.FactAirQualityDaily (
    FactAirQualityDailyKey BIGINT NOT NULL PRIMARY KEY,
    DateKey INT NOT NULL,
    ZoneKey INT NULL,
    PollutantCode NVARCHAR(30) NOT NULL,
    AvgPollutantValue DECIMAL(18, 6) NOT NULL,
    MaxPollutantValue DECIMAL(18, 6) NOT NULL,
    MeasurementCount BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS dbo.FactWeatherDaily (
    FactWeatherDailyKey BIGINT NOT NULL PRIMARY KEY,
    DateKey INT NOT NULL,
    ZoneKey INT NULL,
    AvgTemperatureC DECIMAL(18, 4) NOT NULL,
    MaxTemperatureC DECIMAL(18, 4) NOT NULL,
    MinTemperatureC DECIMAL(18, 4) NOT NULL,
    TotalPrecipitationMM DECIMAL(18, 4) NOT NULL,
    AvgHumidityPct DECIMAL(18, 4) NOT NULL,
    MaxWindSpeedKmh DECIMAL(18, 4) NOT NULL
);

CREATE TABLE IF NOT EXISTS dbo.FactEconomicDaily (
    FactEconomicDailyKey BIGINT NOT NULL PRIMARY KEY,
    DateKey INT NOT NULL,
    FXKey BIGINT NULL,
    GDPKey BIGINT NULL,
    USDToEURRate DECIMAL(18, 6) NULL,
    GDPUsd DECIMAL(38, 2) NULL,
    GDPEurApprox DECIMAL(38, 2) NULL
);
