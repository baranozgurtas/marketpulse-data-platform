-- ============================================================
-- MarketPulse Data Platform — Snowflake Schema Setup
-- ============================================================
-- Run this first to create the database and layered schemas.
-- ============================================================

CREATE DATABASE IF NOT EXISTS MARKETPULSE;

USE DATABASE MARKETPULSE;

-- Medallion architecture: RAW → STAGED → CURATED
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGED;
CREATE SCHEMA IF NOT EXISTS CURATED;
