# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 1.1.x   | :white_check_mark: |
| < 1.1   | Case-by-case basis |

Security fixes for older versions are evaluated on a case-by-case basis depending on the severity of the vulnerability and the effort required to backport the fix.

## Reporting a Vulnerability

### Where to Report

- **Non-sensitive issues**: Open a [GitHub issue](https://github.com/mihiarc/pyfia/issues)
- **Security vulnerabilities**: Contact the maintainer directly via GitHub

### What to Include

When reporting a security vulnerability, please include:

1. A description of the vulnerability
2. Steps to reproduce the issue
3. Potential impact assessment
4. Any suggested fixes (if applicable)
5. Your contact information for follow-up questions

### Response Timeline

- **Acknowledgment**: Within 48-72 hours
- **Initial assessment**: Within 1 week
- **Resolution timeline**: Depends on severity and complexity

### Process

1. **Report received**: You will receive an acknowledgment within 48-72 hours
2. **Triage**: We assess the severity and validity of the report
3. **Investigation**: We investigate the issue and develop a fix
4. **Disclosure**: We coordinate disclosure timing with you
5. **Release**: Security fix is released with appropriate credit (if desired)

## Security Considerations

### SQL Query Execution

pyFIA executes SQL queries against DuckDB databases containing FIA data. The library implements the following safeguards:

- **Input validation**: Domain expressions are validated before execution
- **SQL injection prevention**: Domain filter syntax is parsed and validated to prevent injection attacks
- **Identifier sanitization**: Table and column names are sanitized

### Database File Trust

Users should only use FIA database files from trusted sources:

- Official USDA Forest Service FIA DataMart
- pyFIA's built-in downloader (which retrieves from official sources)
- MotherDuck cloud databases with verified data

**Do not use database files from untrusted sources**, as database files can contain malicious content.

### Best Practices

- Keep pyFIA updated to the latest version
- Verify the integrity of downloaded FIA databases
- Run pyFIA in environments with appropriate access controls
- Review domain expressions before execution if they come from untrusted input
