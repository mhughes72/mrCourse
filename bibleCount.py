from mrjob.job import MRJob
from mrjob.step import MRStep

class SpentByCustomer(MRJob):

    def configure_options(self):
        super(SpentByCustomer, self).configure_options()
        self.add_file_option('--items', help='Path to u.item')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_data,
                   reducer=self.reducer_get_data),
            MRStep(mapper=self.mapper_sort_data,
                   reducer = self.reducer_sort_data)
        ]

    def mapper_get_data(self, _, line):
        (custID, prodID, amount) = line.split(',')
        yield custID, float(amount)

    def reducer_get_data(self, custID, amount):          
        yield custID, '%.02f'%(sum(amount))
        
    def mapper_sort_data(self, custID, amount):
        yield amount, custID

    def reducer_sort_data(self, amount, custID):
        for id in custID:
            yield "$"+str(amount), id
     

if __name__ == '__main__':
    SpentByCustomer.run()
    
    
    