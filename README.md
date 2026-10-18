# Financealerts 2

## Status

The GitHub Actions workflow is disabled. Generated price history, reports,
and watchlists have been removed from the current branch.

To resume processing, enable **Cron run workflow** in GitHub Actions and
run it manually or wait for its schedule. The application will regenerate
`data/` using fresh downloads; the first run may take longer.

1. Takes a big list of tickers

2. Get stock prices

3. compute RSI

4. If RSI > 70 save in short list

5. If RSI < 30 save in long list

6. TODO do LLM tricks to clean short and long list

7. create a CSV list for long and short

8. TODO commit the report to github

9. TODO send telegram notification