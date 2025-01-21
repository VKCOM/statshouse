// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

/** @returns {Promise<import('jest').Config>} */

const jestConfig = async () => {
  process.env.TZ = 'Europe/Moscow';
  return {
    clearMocks: true,
    coveragePathIgnorePatterns: ['/node_modules/'],
    moduleDirectories: ['node_modules'],
    moduleFileExtensions: ['js', 'mjs', 'cjs', 'jsx', 'ts', 'tsx', 'json', 'node'],
    moduleNameMapper: {
      '^@/(.*)$': '<rootDir>/src/$1',
      '^~(.*)$': '<rootDir>/node_modules/$1',
      // '^.+\\.(css|scss)$': '<rootDir>/src/testMock/styleMock.ts',
    },
    setupFilesAfterEnv: ['./src/setupTests.ts'],
    testEnvironment: 'jsdom',
    transform: {
      '^.+\\.(t|j)sx?$': ['@swc/jest'],
      '^.+\\.(css|scss)$': 'jest-css-modules-transform',
    },
    transformIgnorePatterns: ['/node_modules/'],
  };
};

export default jestConfig;
