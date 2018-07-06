from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer(self, movieID, occurences):
        movies=movieID, sum(occurences)
        movies2 = movies[occurances, movieID]
        
        yield movies.sortByValue  
        
if __name__ == '__main__':
    MRRatingCounter.run()
