
CXX                  = g++ -ggdb 
CXXFLAGS                += #-O3
LDFLAGS                 += -lm

OBJS = main.o up.o

c_pro: $(OBJS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS)  $(LDFLAGS) $(OBJS) -o c_pro

%.o: %.cpp %.h
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDES) -c $< -o $@ 

%.o: %.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(INCLUDES) -c $< -o $@
