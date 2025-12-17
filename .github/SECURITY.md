# Security Policy

## Supported Versions

Security updates are provided for the latest stable release.

| Version | Supported          |
| ------- | ------------------ |
| 1.x     | :white_check_mark: |

## Reporting a Vulnerability

**Do not report security vulnerabilities through public GitHub issues.**

Please email security concerns directly to **[contact@flarme.com](mailto:contact@flarme.com)**.

Include in your report:

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Any suggested fixes

### Response Timeline

- **Initial response**: Within 48 hours
- **Status update**: Within 7 days
- **Fix timeline**: Depends on severity

We will coordinate with you on disclosure timing and credit you for your discovery once the issue is resolved.

## Security Best Practices

When using this library:

- Never expose ClickHouse credentials in client-side code
- Use environment variables for sensitive configuration
- Enable HTTPS when connecting over public networks
- Use ClickHouse's built-in authentication and access control
- Validate and sanitize user input before using in queries
