from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from get_indicators import router as get_indicators_router  
from get_periods import router as get_periods_router
from get_segments import router as get_segments_router
from get_index_tree_data import router as get_index_tree_data_router
from get_index_attributes import router as get_index_attributes_router
from testRouter import router as test_router
from new_get_index_tree_data import router as new_get_index_tree_data_router
from save_data import router as save_data_router
from get_data import router as get_data_router
from delete_data import router as delete_data_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(get_indicators_router)   
app.include_router(get_periods_router)  
app.include_router(get_segments_router)   
app.include_router(get_index_tree_data_router) 
app.include_router(get_index_attributes_router) 
app.include_router(test_router) 
app.include_router(new_get_index_tree_data_router) 
app.include_router(save_data_router) 
app.include_router(get_data_router) 
app.include_router(delete_data_router) 
