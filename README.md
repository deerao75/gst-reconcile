# GST Reconcile

A Flask-based web application for GST (Goods and Services Tax) reconciliation and invoice matching.

## What is a Coding Agent?

A **coding agent** is an AI-powered software assistant that can autonomously understand, write, modify, and maintain code in a repository. Unlike traditional code completion tools, coding agents can:

### Key Capabilities

1. **Autonomous Code Changes**: Agents can independently make changes across multiple files to implement features, fix bugs, or refactor code without step-by-step human guidance.

2. **Contextual Understanding**: They analyze the entire codebase, understand project structure, dependencies, coding patterns, and conventions to make informed decisions.

3. **Task Planning**: Given a high-level request, agents break down tasks into steps, create implementation plans, and execute them systematically.

4. **Code Review & Testing**: Agents can run tests, identify failures, debug issues, and iteratively improve code until it meets quality standards.

5. **Documentation**: They can generate or update documentation, READMEs, and code comments based on understanding of the codebase.

### How Coding Agents Work

Coding agents typically follow this workflow:

```
User Request → Understanding → Planning → Implementation → Testing → Refinement → Completion
```

1. **Understanding Phase**: The agent explores the repository, reads relevant files, and understands the current state of the code.

2. **Planning Phase**: Based on the request, the agent creates a step-by-step plan to accomplish the goal with minimal changes.

3. **Implementation Phase**: The agent makes surgical, precise code changes across files as needed.

4. **Testing Phase**: The agent runs builds, tests, and linters to validate changes don't break existing functionality.

5. **Refinement Phase**: If issues are found, the agent iterates to fix them until all tests pass.

6. **Completion Phase**: The agent commits changes and documents what was done.

### Example Use Cases

- **Feature Implementation**: "Add pagination to the reconciliation results table"
- **Bug Fixes**: "Fix the date parsing issue in Excel imports"
- **Refactoring**: "Extract the GSTIN validation logic into a separate module"
- **Documentation**: "Create API documentation for all Flask routes"
- **Code Cleanup**: "Remove duplicate imports and unused variables"
- **Testing**: "Add unit tests for the reconciliation algorithm"

### Benefits for This Project

For the GST Reconcile application, a coding agent can:

- Add new reconciliation features without disrupting existing functionality
- Fix bugs identified in production
- Optimize performance of large dataset processing
- Improve code organization and maintainability
- Add missing documentation
- Ensure security best practices
- Update dependencies safely

### Limitations

Coding agents work best when:
- The codebase has clear structure and conventions
- Tests exist to validate changes
- Requests are specific and well-defined
- The repository has good documentation

They may struggle with:
- Highly ambiguous requirements
- Complex architectural decisions requiring human judgment
- Tasks requiring external domain expertise not present in the code

## About This Application

This Flask application helps businesses reconcile their GST invoices by:
- Uploading and parsing Excel/CSV files with invoice data
- Matching invoices from different sources (2A, 2B formats)
- Identifying discrepancies in amounts, dates, and GSTIN
- Generating detailed reconciliation reports
- Providing a dashboard for reconciliation status

## Getting Started

1. Install dependencies: `pip install -r requirements.txt`
2. Set up environment variables in `.env`
3. Run the application: `python main.py` or `gunicorn main:app`

## Features

- **File Upload**: Support for Excel and CSV formats
- **Intelligent Column Mapping**: Automatic detection of GSTIN, invoice numbers, amounts, and dates
- **Reconciliation Engine**: Match invoices across different data sources
- **Dashboard**: Visual representation of reconciliation status
- **Background Processing**: Queue-based processing for large files
- **Google Drive Integration**: Backup and storage of reconciliation results

## Technology Stack

- **Backend**: Flask (Python)
- **Database**: SQLite/PostgreSQL with SQLAlchemy
- **Task Queue**: Redis + RQ (Redis Queue)
- **Data Processing**: Pandas, NumPy
- **File Handling**: openpyxl, xlsxwriter
- **Cloud Storage**: Google Drive API
- **Authentication**: Flask-SQLAlchemy User model

## Contributing

When contributing to this project, coding agents can help by:
- Following existing code patterns
- Maintaining consistency in naming conventions
- Adding tests for new features
- Updating documentation
- Ensuring security best practices
