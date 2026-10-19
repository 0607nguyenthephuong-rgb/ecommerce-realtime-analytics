# Upload this starter to GitHub

Suggested repository name: `ecommerce-behavior-streaming`.

Suggested description:

> A Big Data Processing project for replaying and analyzing cosmetics e-commerce user events with Kafka, Spark Structured Streaming, Hadoop HDFS, and a planned real-time dashboard.

## Using Git

1. Extract the archive and open a terminal inside `ecommerce-behavior-streaming`.
2. Create an empty repository in your GitHub account.
3. Review the files, then run the commands below using your real repository URL.

```bash
git init
git add .
git status
git commit -m "Initialize e-commerce analytics project"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/ecommerce-behavior-streaming.git
git push -u origin main
```

If the remote already contains commits, clone it first and copy these files into the clone. Do not overwrite unrelated files or force-push to solve an initialization conflict.

## Using the GitHub website

Open the target repository, choose **Add file → Upload files**, and upload the extracted repository contents. Keep `README.md` at the repository root instead of uploading the outer folder or the ZIP itself. Include the hidden `.github/`, `.gitignore`, and `.gitattributes` entries; using Git is usually more reliable for preserving them.

## Before the first commit

- Review `git status` and confirm no raw dataset, credentials, or runtime artifacts are included.
- Update member GitHub usernames when known; the starter deliberately contains no guessed mentions.
- Create the four milestone entries and initial tasks from `PROJECT_PLAN.md`.
- Keep the dataset attribution links in the README and source documentation.

