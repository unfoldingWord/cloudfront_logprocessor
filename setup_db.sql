-- phpMyAdmin SQL Dump
-- version 5.1.0
-- https://www.phpmyadmin.net/
--
-- Host: mariadb:3306
-- Generation Time: Oct 06, 2021 at 12:44 PM
-- Server version: 10.6.4-MariaDB-1:10.6.4+maria~focal
-- PHP Version: 7.4.16

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `log_store`
--

-- --------------------------------------------------------

--
-- Table structure for table `cloudfront_log`
--

CREATE TABLE `cloudfront_log` (
  `log_id` bigint(20) UNSIGNED NOT NULL,
  `date` date NOT NULL,
  `time` time NOT NULL,
  `x-edge-location` tinytext COLLATE utf8mb4_unicode_ci NOT NULL,
  `sc-bytes` int(10) UNSIGNED NOT NULL,
  `c-ip` tinytext COLLATE utf8mb4_unicode_ci NOT NULL,
  `cs-method` tinytext COLLATE utf8mb4_unicode_ci NOT NULL,
  `cs(Host)` tinytext COLLATE utf8mb4_unicode_ci NOT NULL,
  `cs-uri-stem` tinytext COLLATE utf8mb4_unicode_ci NOT NULL,
  `sc-status` smallint(5) UNSIGNED NOT NULL,
  `cs(Referer)` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `cs(User-Agent)` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `cs-uri-query` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `cs(Cookie)` text COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `x-edge-result-type` tinytext COLLATE utf8mb4_unicode_ci NOT NULL,
  `x-edge-request-id` tinytext COLLATE utf8mb4_unicode_ci NOT NULL,
  `x-host-header` tinytext COLLATE utf8mb4_unicode_ci NOT NULL,
  `cs-protocol` tinytext COLLATE utf8mb4_unicode_ci NOT NULL,
  `cs-bytes` smallint(5) UNSIGNED NOT NULL,
  `time-taken` float UNSIGNED NOT NULL,
  `x-forwarded-for` tinytext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ssl-protocol` tinytext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ssl-cipher` tinytext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `x-edge-response-result-type` tinytext COLLATE utf8mb4_unicode_ci NOT NULL,
  `cs-protocol-version` tinytext COLLATE utf8mb4_unicode_ci NOT NULL,
  `fle-status` tinytext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `fle-encrypted-fields` smallint(5) UNSIGNED DEFAULT NULL,
  `c-port` smallint(5) UNSIGNED DEFAULT NULL,
  `time-to-first-byte` float UNSIGNED DEFAULT NULL,
  `x-edge-detailed-result-type` tinytext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sc-content-type` tinytext COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sc-content-len` int(10) UNSIGNED DEFAULT NULL,
  `sc-range-start` int(10) UNSIGNED DEFAULT NULL,
  `sc-range-end` int(10) UNSIGNED DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Indexes for table `cloudfront_log`
--
ALTER TABLE `cloudfront_log`
  ADD PRIMARY KEY (`log_id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `cloudfront_log`
--
ALTER TABLE `cloudfront_log`
  MODIFY `log_id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;