
# $1 is the date to check changes from
# use format YYYY-MM-DD

echo "Finding errors newer than: $1"
echo "--------------------------------------"

for f in `find /home/fksb/mink-dev-data/lexicon/ -name mink.out -newermt "$1"`; do
  error=`grep ERROR $f`
  if grep -q ERROR "$f"; then
    echo $(dirname -- $f | xargs basename)
    # TODO only find errors with unspecified reason
    if grep -q version "$f"; then
      echo "error for " `grep version $f | jq -r '.message'`
    else
      echo "error in karp-pipeline (unknown version)"
    fi
    echo `echo $error | jq -r .message`
    echo "--------------------------------------"
  fi
done;

